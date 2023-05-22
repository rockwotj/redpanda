use bridge::ValueType;
use cxx::UniquePtr;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use futures::future::BoxFuture;
use futures::Future;
use anyhow::Result;

#[cxx::bridge(namespace = "wasmstar")]
mod bridge {
    enum ValueType {
        I32,
        I64,
        F32,
        F64,
    }

    extern "Rust" {
        type Engine;
        unsafe fn create_engine() -> Result<Box<Engine>>;
        type Module;
        unsafe fn compile_module(self: &mut Engine, wat: &str) -> Result<Box<Module>>;
        type Store;
        unsafe fn create_store(self: &mut Engine) -> Result<Box<Store>>;
        type Instance;
        unsafe fn create_instance(
            self: &mut Store,
            engine: &Engine,
            module: &Module,
        ) -> Result<Box<Instance>>;
        type FunctionHandle;
        unsafe fn lookup_function(
            self: &mut Instance,
            store: &mut Store,
            func_name: &str,
        ) -> Result<Box<FunctionHandle>>;
        unsafe fn invoke<'a>(
            self: &'a mut FunctionHandle,
            store: &'a mut Store,
        ) -> Box<RunningFunction<'a>>;

        type RunningFunction<'a>;
        // Pump the function and return if it finished or not
        unsafe fn pump(self: &mut RunningFunction<'_>) -> Result<bool>;
    }
    unsafe extern "C++" {
        include!("ffi.h");
        type AsyncResult;
        fn poll_async_result(future: &UniquePtr<AsyncResult>) -> Result<bool>;
        fn consume_u32_async_value(future: UniquePtr<AsyncResult>) -> Result<u32>;
        fn consume_u64_async_value(future: UniquePtr<AsyncResult>) -> Result<u64>;

        type HostContext;
        fn call_host_fn(
            module: &str,
            name: &str,
            ctx: UniquePtr<HostContext>,
            params: &[u64],
            result: &[u64],
        ) -> UniquePtr<AsyncResult>;
    }
}

unsafe impl Send for bridge::AsyncResult {}

impl bridge::ValueType {
    fn as_wasmtime_type(self) -> wasmtime::ValType {
        match self {
            ValueType::I32 => wasmtime::ValType::I32,
            ValueType::I64 => wasmtime::ValType::I64,
            ValueType::F32 => wasmtime::ValType::F32,
            ValueType::F64 => wasmtime::ValType::F64,
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
            module: wasmtime::Module::new(&self.engine, wat)?,
        }));
    }

    fn create_store(self: &mut Engine) -> Result<Box<Store>> {
        let mut store = wasmtime::Store::new(&self.engine, ());
        // If you want to see what happens when fuel is exhausted make these numbers really low.
        store.out_of_fuel_async_yield(
            /*injection_count=*/ 100_000_000_000,
            /*fuel_to_inject=*/ 100_000_000,
        );
        return Ok(Box::new(Store { store }));
    }
}

pub struct Module {
    module: wasmtime::Module,
}

pub struct Store {
    store: wasmtime::Store<()>,
}

pub struct Linker {
    linker: wasmtime::Linker<()>,
}

pub struct AsyncResultAdapter {
    inner: cxx::UniquePtr<bridge::AsyncResult>,
}

impl AsyncResultAdapter {
    fn consume_u32_value(self) -> Result<u32> {
        bridge::consume_u32_async_value(self.inner).map_err(|err| wasmtime::Error::new(err))
    }

    fn consume_u64_value(self) -> Result<u64> {
        bridge::consume_u64_async_value(self.inner).map_err(|err| wasmtime::Error::new(err))
    }
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

impl Engine {
    fn create_linker(&mut self) -> Box<Linker> {
        Box::new(Linker {
            linker: wasmtime::Linker::new(&self.engine),
        })
    }
}

impl Linker {
    fn add_host_fn(
        &mut self,
        module: &str,
        name: &str,
        ctx: UniquePtr<bridge::HostContext>,
        params: &[ValueType],
        results: &[ValueType],
    ) -> Result<()> {
        self.linker.func_new_async(
            module,
            name,
            wasmtime::FuncType::new(
                params.iter().map(|t| t.as_wasmtime_type()),
                results.iter().map(|t| t.as_wasmtime_type()),
            ),
            |caller, params, results| Box::new(),
        )?;
    }
}

impl Store {
    fn create_instance(
        self: &mut Store,
        engine: &Engine,
        module: &Module,
    ) -> Result<Box<Instance>> {
        let mut linker = wasmtime::Linker::new(&engine.engine);
        linker.func_new_async(
            "host",
            "sleep",
            wasmtime::FuncType::new(None, None),
            |_caller, _params, _results| async_sleep(),
        )?;
        let mut instance_future =
            Box::pin(linker.instantiate_async(&mut self.store, &module.module));
        // TODO: Make creating an engine async too, until then, just poll until completion
        let mut noop_waker = core::task::Context::from_waker(futures::task::noop_waker_ref());
        loop {
            match instance_future.as_mut().poll(&mut noop_waker) {
                Poll::Pending => {}
                Poll::Ready(Ok(instance)) => return Ok(Box::new(Instance { instance })),
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
    fn lookup_function(
        self: &mut Instance,
        store: &mut Store,
        func_name: &str,
    ) -> Result<Box<FunctionHandle>> {
        let handle = self
            .instance
            .get_typed_func::<(), ()>(&mut store.store, &func_name)
            .unwrap();
        return Ok(Box::new(FunctionHandle { handle }));
    }
}

pub struct FunctionHandle {
    handle: wasmtime::TypedFunc<(), ()>,
}

impl FunctionHandle {
    fn invoke<'a>(self: &'a mut FunctionHandle, store: &'a mut Store) -> Box<RunningFunction<'a>> {
        return Box::new(RunningFunction {
            fut: Box::pin(self.handle.call_async(&mut store.store, ())),
        });
    }
}

pub struct RunningFunction<'a> {
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
}
