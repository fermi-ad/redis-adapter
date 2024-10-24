#pragma once

#if __has_include(<span>) && defined(__cpp_lib_span)
#include <span>
#else
//  a very simple std::span

namespace std
{
  template<typename T, size_t _DUMMY_ = 0> class span
  {
  public:
    span( T* buf, size_t sz) : _buf(buf), _sz(sz) {}
    T * data() const { return _buf; }
    const size_t size() const { return _sz; }
    T * begin() const { return _buf; }
    T*  end() const { return _buf + _sz; }

  private:
    T * const _buf;
    const size_t _sz;
  };
};
#endif