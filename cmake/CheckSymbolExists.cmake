# override the cmake provided version of this macro
# always set var to TRUE
macro(check_symbol_exists _symbol _file _var)
  set(${_var} TRUE PARENT_SCOPE)
endmacro()
