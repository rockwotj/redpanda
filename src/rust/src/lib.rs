use anyhow::anyhow;
use anyhow::Result;
use bridge::ValueType;
use wasmtime::AsContext;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use futures::future::BoxFuture;
use futures::Future;

#[cxx::bridge(namespace = "wasmstar")]
mod bridge {
    enum ValueType {
        I32,
        I64,
        F32,
        F64,
    }

    unsafe extern "C++" {
        include!("ffi.h");
        type AsyncResult;
        fn poll_async_result(future: &UniquePtr<AsyncResult>) -> Result<bool>;

        type HostFunc;
        fn invoke(
            func: &UniquePtr<HostFunc>,
            memory: &mut [u8],
            params: &[u64],
            results: &mut [u64],
        ) -> i32;
    }

    extern "Rust" {
        type Engine;
        fn create_engine() -> Result<Box<Engine>>;
        fn create_linker(self: &mut Engine) -> Box<Linker>;

        type Linker;
        unsafe fn register_host_fn(
            self: &mut Linker,
            module: &str,
            name: &str,
            param_types: Vec<ValueType>,
            result_types: Vec<ValueType>,
            func: UniquePtr<HostFunc>,
        ) -> Result<()>;

        type Module;
        fn compile_module(self: &mut Engine, wat: &str) -> Result<Box<Module>>;

        type Store;
        fn create_store(self: &mut Engine) -> Result<Box<Store>>;

        type Instance;
        fn create_instance(
            self: &mut Store,
            linker: &Linker,
            module: &Module,
        ) -> Result<Box<Instance>>;
        fn register_memory(
            self: &mut Instance,
            store: &mut Store,
        ) -> Result<()>;

        type FunctionHandle;
        fn lookup_function(
            self: &mut Instance,
            store: &mut Store,
            func_name: &str,
            param_types: Vec<ValueType>,
            result_types: Vec<ValueType>,
        ) -> Result<Box<FunctionHandle>>;
        unsafe fn invoke<'a>(
            self: &'a mut FunctionHandle,
            store: &'a mut Store,
            params: &[u64],
        ) -> Result<Box<RunningFunction<'a>>>;

        type RunningFunction<'a>;
        // Pump the function and return if it finished or not
        fn pump(self: &mut RunningFunction<'_>) -> Result<bool>;
        unsafe fn results(self: &RunningFunction<'_>) -> Result<Vec<u64>>;
    }
}

unsafe impl Send for bridge::AsyncResult {}

struct StoreContext {
    memory: Option<wasmtime::Memory>,
}

impl bridge::ValueType {
    fn as_wasmtime_type(&self) -> Result<wasmtime::ValType> {
        match *self {
            ValueType::I32 => Ok(wasmtime::ValType::I32),
            ValueType::I64 => Ok(wasmtime::ValType::I64),
            ValueType::F32 => Ok(wasmtime::ValType::F32),
            ValueType::F64 => Ok(wasmtime::ValType::F64),
            _ => Err(anyhow!("Unexpected ValueType: {}", self.repr)),
        }
    }
}

pub struct Engine {
    engine: wasmtime::Engine,
}

fn create_engine() -> Result<Box<Engine>> {
    let mut config = wasmtime::Config::new();
    config.async_support(true);
    config.consume_fuel(true);
    // This tells wasmtime to have dynamic memory usage of just what is needed.
    config.static_memory_maximum_size(0);
    config.dynamic_memory_reserved_for_growth(0);
    config.dynamic_memory_guard_size(0);
    config.max_wasm_stack(128 * 1024);
    config.async_stack_size(256 * 1024);
    config.cranelift_opt_level(wasmtime::OptLevel::None);
    let engine = wasmtime::Engine::new(&config)?;
    return Ok(Box::new(Engine { engine }));
}

impl Engine {
    fn compile_module(self: &mut Engine, wat: &str) -> Result<Box<Module>> {
        return Ok(Box::new(Module {
            underlying: wasmtime::Module::new(&self.engine, wat)?,
        }));
    }

    fn create_store(self: &mut Engine) -> Result<Box<Store>> {
        let mut store = wasmtime::Store::new(&self.engine, StoreContext { memory: None });
        // If you want to see what happens when fuel is exhausted make these numbers really low.
        store.out_of_fuel_async_yield(
            /*injection_count=*/ 100_000_000_000,
            /*fuel_to_inject=*/ 100_000_000,
        );
        return Ok(Box::new(Store { underlying: store }));
    }
}

pub struct Module {
    underlying: wasmtime::Module,
}

pub struct Store {
    underlying: wasmtime::Store<StoreContext>,
}

pub struct AsyncResultAdapter {
    inner: cxx::UniquePtr<bridge::AsyncResult>,
}

impl Future for AsyncResultAdapter {
    type Output = Result<()>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
        match bridge::poll_async_result(&self.inner) {
            Ok(true) => Poll::Ready(Ok(())),
            Ok(false) => Poll::Pending,
            Err(x) => Poll::Ready(Err(x.into())),
        }
    }
}

pub struct Linker {
    underlying: wasmtime::Linker<StoreContext>,
}

impl Engine {
    fn create_linker(&mut self) -> Box<Linker> {
        Box::new(Linker {
            underlying: wasmtime::Linker::new(&self.engine),
        })
    }
}

unsafe impl Send for bridge::HostFunc {}
unsafe impl Sync for bridge::HostFunc {}

impl Linker {
    unsafe fn register_host_fn(
        &mut self,
        module: &str,
        name: &str,
        param_types: Vec<ValueType>,
        result_types: Vec<ValueType>,
        func: cxx::UniquePtr<bridge::HostFunc>,
    ) -> Result<()> {
        let module_name = module.to_owned();
        let function_name = name.to_owned();
        self.underlying.func_new(
            module,
            name,
            wasmtime::FuncType::new(
                param_types
                    .iter()
                    .map(|t| t.as_wasmtime_type())
                    .collect::<Result<Vec<_>>>()?,
                result_types
                    .iter()
                    .map(|t| t.as_wasmtime_type())
                    .collect::<Result<Vec<_>>>()?,
            ),
            move |caller, params, results| {
                let ffi_params = params
                    .iter()
                    .map(|v| match v {
                        wasmtime::Val::I32(i) => Ok(*i as u64),
                        wasmtime::Val::I64(i) => Ok(*i as u64),
                        wasmtime::Val::F32(f) => Ok(*f as u64),
                        wasmtime::Val::F64(f) => Ok(*f),
                        _ => Err(anyhow!("Unexpected value type in host call: {}", v.ty())),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut ffi_results: Vec<u64> = std::iter::repeat(0).take(results.len()).collect();
                let mut mem = match &caller.data().memory {
                    // This again is another hack to trick the borrow checker into what we want to
                    // do here. The wasmtime API makes it very difficult to safely get mutable memory in
                    // a host call.
                    Some(m) => unsafe {
                        std::slice::from_raw_parts_mut(
                            m.data_ptr(caller.as_context()),
                            m.data_size(caller.as_context()))
                    },
                    None => {
                        return Err(anyhow!(
                            "Invariant failure, memory not initialized for host call."
                        ))
                    },
                };
                let errc = bridge::invoke(&func, &mut mem, &ffi_params, &mut ffi_results);
                if errc < 0 {
                    return Err(anyhow!(
                        "Error calling host function {}.{}, error code: {}",
                        module_name,
                        function_name,
                        errc
                    ));
                }
                for (idx, (val, typ)) in
                    ffi_results.into_iter().zip(result_types.iter()).enumerate()
                {
                    results[idx] = match *typ {
                        bridge::ValueType::I32 => wasmtime::Val::I32(val as i32),
                        bridge::ValueType::I64 => wasmtime::Val::I64(val as i64),
                        bridge::ValueType::F32 => wasmtime::Val::F32(val as u32),
                        bridge::ValueType::F64 => wasmtime::Val::F64(val),
                        _ => return Err(anyhow!("Unexpected ValueType: {}", typ.repr)),
                    }
                }
                Ok(())
            },
        )?;
        Ok(())
    }
}

impl Store {
    fn create_instance(
        self: &mut Store,
        linker: &Linker,
        module: &Module,
    ) -> Result<Box<Instance>> {
        let mut instance_future = Box::pin(
            linker
                .underlying
                .instantiate_async(&mut self.underlying, &module.underlying),
        );
        // TODO: Make creating an engine async too, until then, just poll until completion
        let mut noop_waker = core::task::Context::from_waker(futures::task::noop_waker_ref());
        loop {
            match instance_future.as_mut().poll(&mut noop_waker) {
                Poll::Pending => {}
                Poll::Ready(Ok(instance)) => {
                    return Ok(Box::new(Instance { instance }));
                }
                Poll::Ready(Err(e)) => {
                    return Err(e);
                }
            }
        }
    }
}

pub struct Instance {
    instance: wasmtime::Instance,
}

impl Instance {
    fn register_memory(self: &mut Instance, store: &mut Store) -> Result<()> {
        let mem = self.instance.get_memory(&mut store.underlying, "memory");
        match mem {
            Some(m) => store.underlying.data_mut().memory = Some(m),
            None => return Err(anyhow!("Invalid module missing memory")),
        }
        return Ok(());
    }

    fn lookup_function(
        self: &mut Instance,
        store: &mut Store,
        func_name: &str,
        param_types: Vec<bridge::ValueType>,
        result_types: Vec<bridge::ValueType>,
    ) -> Result<Box<FunctionHandle>> {
        let handle = self
            .instance
            .get_func(&mut store.underlying, &func_name)
            .unwrap();
        // Validate the signature is correct
        let ty = handle.ty(&store.underlying);
        if ty.params().len() != param_types.len() {
            return Err(anyhow!(
                "Function {} has incorrect parameter signature arity {}",
                func_name,
                ty.params().len()
            ));
        }
        for (actual, expected) in ty.params().zip(param_types.iter()) {
            let is_match = *expected
                == match actual {
                    wasmtime::ValType::I32 => bridge::ValueType::I32,
                    wasmtime::ValType::I64 => bridge::ValueType::I64,
                    wasmtime::ValType::F32 => bridge::ValueType::F32,
                    wasmtime::ValType::F64 => bridge::ValueType::F64,
                    _ => {
                        return Err(anyhow!(
                            "Unexpected parameter type for {}: {}",
                            func_name,
                            actual
                        ))
                    }
                };
            if !is_match {
                return Err(anyhow!(
                    "Function {} has incorrect parameter signature {}",
                    func_name,
                    actual
                ));
            }
        }
        if ty.results().len() != result_types.len() {
            return Err(anyhow!(
                "Function {} has incorrect result signature arity {}",
                func_name,
                ty.params().len()
            ));
        }
        for (actual, expected) in ty.results().zip(result_types.iter()) {
            let is_match = *expected
                == match actual {
                    wasmtime::ValType::I32 => bridge::ValueType::I32,
                    wasmtime::ValType::I64 => bridge::ValueType::I64,
                    wasmtime::ValType::F32 => bridge::ValueType::F32,
                    wasmtime::ValType::F64 => bridge::ValueType::F64,
                    _ => {
                        return Err(anyhow!(
                            "Unexpected result type for {}: {}",
                            func_name,
                            actual
                        ))
                    }
                };
            if !is_match {
                return Err(anyhow!(
                    "Function {} has incorrect result signature {}",
                    func_name,
                    actual
                ));
            }
        }
        return Ok(Box::new(FunctionHandle {
            param_types,
            result_types,
            handle,
        }));
    }
}

pub struct FunctionHandle {
    param_types: Vec<bridge::ValueType>,
    result_types: Vec<bridge::ValueType>,
    handle: wasmtime::Func,
}

impl FunctionHandle {
    fn invoke<'a>(
        self: &'a mut FunctionHandle,
        store: &'a mut Store,
        params: &[u64],
    ) -> Result<Box<RunningFunction<'a>>> {
        if params.len() != self.param_types.len() {
            return Err(anyhow!(
                "Invalid number of parameters to function: {}",
                params.len()
            ));
        }
        let mut running = Box::new(RunningFunction {
            params: self
                .param_types
                .iter()
                .zip(params.iter())
                .map(|(ty, v)| match *ty {
                    bridge::ValueType::I32 => Ok(wasmtime::Val::I32(*v as i32)),
                    bridge::ValueType::I64 => Ok(wasmtime::Val::I64(*v as i64)),
                    bridge::ValueType::F32 => Ok(wasmtime::Val::F32(*v as u32)),
                    bridge::ValueType::F64 => Ok(wasmtime::Val::F64(*v)),
                    _ => Err(anyhow!("Unexpected value type in host call: {}", ty.repr)),
                })
                .collect::<Result<Vec<_>, _>>()?,
            results: std::iter::repeat(wasmtime::Val::I32(0))
                .take(self.result_types.len())
                .collect(),
            fut: Box::pin(std::future::ready(Ok(()))),
        });
        running.fut = Box::pin(unsafe {
            // Trick the borrow checker into thinking this is safe (it happens to be), by roundtripping
            // through a pointer.
            let p = std::slice::from_raw_parts(running.params.as_ptr(), running.params.len());
            let r =
                std::slice::from_raw_parts_mut(running.results.as_mut_ptr(), running.results.len());
            self.handle.call_async(&mut store.underlying, p, r)
        });
        return Ok(running);
    }
}

struct RunningFunction<'a> {
    params: Vec<wasmtime::Val>,
    results: Vec<wasmtime::Val>,
    fut: BoxFuture<'a, Result<()>>,
}

impl<'a> RunningFunction<'a> {
    fn pump(&mut self) -> Result<bool> {
        let mut noop_waker = core::task::Context::from_waker(futures::task::noop_waker_ref());
        match self.fut.as_mut().poll(&mut noop_waker) {
            Poll::Pending => Ok(false),
            Poll::Ready(Ok(())) => Ok(true),
            Poll::Ready(Err(e)) => Err(e),
        }
    }

    fn results(&self) -> Result<Vec<u64>> {
        let mut ffi_results = Vec::with_capacity(self.results.len());
        for val in &self.results {
            ffi_results.push(match *val {
                wasmtime::Val::I32(i) => i as u64,
                wasmtime::Val::I64(i) => i as u64,
                wasmtime::Val::F32(f) => f as u64,
                wasmtime::Val::F64(f) => f,
                _ => return Err(anyhow!("Unexpected ValueType: {}", val.ty())),
            });
        }
        return Ok(ffi_results);
    }
}
