# override the cmake provided version of this macro
# always set var to 1
macro(check_symbol_exists _symbol _file _var)
  set(${_var} 1 CACHE INTERNAL "Have symbol ${_symbol}")
endmacro()
