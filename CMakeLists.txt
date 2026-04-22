cmake_minimum_required(VERSION 3.20)

set(TARGET_NAME merutable)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# ── merutable-capi: find pre-built static library ────────────────────────────
# Build the Rust crate first:
#   cd /path/to/merutable && cargo build --package merutable-capi --release
#
# MERUTABLE_WORKSPACE: path to the merutable Rust workspace root.
# Override: cmake -DMERUTABLE_WORKSPACE=... or env var MERUTABLE_WORKSPACE.
if(NOT DEFINED MERUTABLE_WORKSPACE)
    if(DEFINED ENV{MERUTABLE_WORKSPACE})
        set(MERUTABLE_WORKSPACE $ENV{MERUTABLE_WORKSPACE})
    else()
        set(MERUTABLE_WORKSPACE "${CMAKE_CURRENT_SOURCE_DIR}/../merutable/merutable")
    endif()
endif()
message(STATUS "merutable workspace: ${MERUTABLE_WORKSPACE}")

# Default: release build. Override with -DMERUTABLE_PROFILE=debug.
if(NOT DEFINED MERUTABLE_PROFILE)
    set(MERUTABLE_PROFILE "release")
endif()

set(MERUTABLE_LIB_DIR "${MERUTABLE_WORKSPACE}/target/${MERUTABLE_PROFILE}")
set(MERUTABLE_LIB     "${MERUTABLE_LIB_DIR}/libmerutable_capi.a")
set(MERUTABLE_INCLUDE "${MERUTABLE_WORKSPACE}/crates/merutable-capi/include")

if(NOT EXISTS "${MERUTABLE_LIB}")
    message(FATAL_ERROR
        "merutable-capi static library not found at '${MERUTABLE_LIB}'.\n"
        "Build it first:\n"
        "  cd ${MERUTABLE_WORKSPACE}\n"
        "  cargo build --package merutable-capi --release\n"
        "Or pass -DMERUTABLE_PROFILE=debug for a debug build.")
endif()

add_library(merutable_capi STATIC IMPORTED)
set_target_properties(merutable_capi PROPERTIES
    IMPORTED_LOCATION "${MERUTABLE_LIB}"
)

# ── Extension sources ─────────────────────────────────────────────────────────
include_directories(src/include "${MERUTABLE_INCLUDE}")

set(EXTENSION_SOURCES
    src/merutable_extension.cpp
    src/handle_cache.cpp
    src/merutable_scan.cpp
    src/merutable_get.cpp
    src/merutable_copy.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# ── Link: Rust static lib + required system libs ──────────────────────────────
if(APPLE)
    find_library(FW_SECURITY Security REQUIRED)
    find_library(FW_COREFOUNDATION CoreFoundation REQUIRED)
    find_library(FW_SYSTEMCONFIGURATION SystemConfiguration REQUIRED)
endif()

foreach(target ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
    target_link_libraries(${target} merutable_capi
        $<$<PLATFORM_ID:Linux>:pthread dl>
        $<$<PLATFORM_ID:Darwin>:${FW_SECURITY}>
        $<$<PLATFORM_ID:Darwin>:${FW_COREFOUNDATION}>
        $<$<PLATFORM_ID:Darwin>:${FW_SYSTEMCONFIGURATION}>
    )
endforeach()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
