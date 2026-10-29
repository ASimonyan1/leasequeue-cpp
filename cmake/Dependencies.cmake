# Exact header versions and SHA-256 checksums. No system-wide installation.
set(WEB_DEPS_DIR "${CMAKE_BINARY_DIR}/web-deps" CACHE PATH "HTTP/JSON header directory")
file(MAKE_DIRECTORY "${WEB_DEPS_DIR}/nlohmann")
function(header name url digest)
  set(destination "${WEB_DEPS_DIR}/${name}")
  if(NOT EXISTS "${destination}")
    file(DOWNLOAD "${url}" "${destination}" EXPECTED_HASH SHA256=${digest}
      TLS_VERIFY ON STATUS result)
    list(GET result 0 code)
    if(NOT code EQUAL 0)
      message(FATAL_ERROR "Dependency download failed: ${result}")
    endif()
  endif()
  file(SHA256 "${destination}" actual)
  if(NOT actual STREQUAL digest)
    message(FATAL_ERROR "Checksum mismatch: ${name}")
  endif()
endfunction()
header(httplib.h https://raw.githubusercontent.com/yhirose/cpp-httplib/v0.63.0/httplib.h
  9021001a719768893a8a6e4051c56107690e8b8e49b41e5f903534fa19ae9afb)
header(nlohmann/json.hpp https://raw.githubusercontent.com/nlohmann/json/v3.12.0/single_include/nlohmann/json.hpp
  aaf127c04cb31c406e5b04a63f1ae89369fccde6d8fa7cdda1ed4f32dfc5de63)
add_library(web_dependencies INTERFACE)
target_include_directories(web_dependencies SYSTEM INTERFACE "${WEB_DEPS_DIR}")
