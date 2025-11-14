cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME sshfs)

project(${TARGET_NAME})

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package.
if(CLANG_TIDY)
  find_package(OpenSSL)
  find_package(Libssh2)
else()
  find_package(OpenSSL REQUIRED)
  find_package(Libssh2 REQUIRED)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

include_directories(src/include)

set(EXTENSION_SOURCES
    src/sshfs_extension.cpp
    src/sshfs_filesystem.cpp
    src/sshfs_file_handle.cpp
    src/ssh_client.cpp
    src/ssh_secrets.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL and libssh2 in both the static library as the loadable extension
if(NOT CLANG_TIDY)
  target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto Libssh2::libssh2)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto Libssh2::libssh2)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
