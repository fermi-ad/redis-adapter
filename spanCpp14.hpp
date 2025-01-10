#pragma once

//  if there is a span header to include
#if __has_include(<span>)
//  include that span header
#include <span>
//  the span header should define __cpp_lib_span
//  if it provides an implementation of std::span
#endif

//  if the above did not define __cpp_lib_span
//  provide our own implementation of std::span
#ifndef __cpp_lib_span
//  a very simple std::span
namespace std
{
  template<typename T, size_t _DUMMY_ = 0> class span
  {
  public:
    span(T* buf, size_t sz) : _buf(buf), _sz(sz) {}
    span(T* beg, T* end) : _buf(beg), _sz(end - beg) {}
    T* data() const { return _buf; }
    const size_t size() const { return _sz; }
    T* begin() const { return _buf; }
    T* end() const { return _buf + _sz; }
  private:
    T* const _buf;
    const size_t _sz;
  };
}
#endif

//  if there is a string_view header to include
#if __has_include(<string_view>)
//  include that string_view header
#include <string_view>
//  the string_view header should define __cpp_lib_string_view
//  if it provides an implementation of std::string_view
#endif

//  if the above did not define __cpp_lib_string_view
//  provide our own implementation of std::string_view
#ifndef __cpp_lib_string_view
//  alias string_view to string
namespace std
{
  using string_view = string;
}
#endif
