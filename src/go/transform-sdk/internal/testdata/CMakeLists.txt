set(GOPATH ${CMAKE_CURRENT_BINARY_DIR})
execute_process(COMMAND ${CMAKE_COMMAND} -E env 
  ${CMAKE_GO_BINARY} env GOROOT 
  OUTPUT_VARIABLE GOROOT 
  OUTPUT_STRIP_TRAILING_WHITESPACE)

function(add_wasm_transform NAME)
  find_program(TINYGO_BIN "tinygo")
  get_filename_component(MAIN_SRC_ABS ${NAME}/transform.go ABSOLUTE)
  set(WASM_OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${NAME}.wasm")
  message(WASM_OUTPUT="${WASM_OUTPUT} GOROOT=${GOROOT} GOPATH=${GOPATH}")
  add_custom_target(wasm_testdata_${NAME})
  add_custom_command(TARGET wasm_testdata_${NAME}
                    COMMAND env GOPATH="${GOPATH}" GOROOT="${GOROOT}" 
                    ${TINYGO_BIN} -o ${WASM_OUTPUT}
                    ${NAME}/transform.go
                    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
                    DEPENDS ${MAIN_SRC_ABS})
  add_custom_target(wasm_testdata_${NAME}_all ALL DEPENDS wasm_testdata_${NAME})
endfunction(add_wasm_transform)

add_wasm_transform(identity) 
add_wasm_transform(transform-error) 
add_wasm_transform(transform-panic) 
add_wasm_transform(setup-panic) 
add_wasm_transform(wasi) 

