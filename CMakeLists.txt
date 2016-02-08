cmake_minimum_required(VERSION 2.8)

project (o5mindexer)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/bin")

if(CMAKE_COMPILER_IS_GNUCXX)
	#set(CMAKE_BUILD_TYPE Debug)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif()


file(GLOB_RECURSE O5MSOURCES src/*.h src/*.c src/*.cpp src/*.cc)

foreach(f ${O5MSOURCES})
    # Get the path of the file relative to ${DIRECTORY},
    # then alter it (not compulsory)
    file(RELATIVE_PATH SRCGR ${CMAKE_CURRENT_SOURCE_DIR} ${f})
    set(SRCGR "${SRCGR}")

    # Extract the folder, ie remove the filename part
    string(REGEX REPLACE "(.*)(/[^/]*)$" "\\1" SRCGR ${SRCGR})

    # Source_group expects \\ (double antislash), not / (slash)
    string(REPLACE / \\ SRCGR ${SRCGR})
    source_group("${SRCGR}" FILES ${f})
endforeach()

if(MSVC)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /MP")
	add_definitions( -D_CRT_SECURE_NO_WARNINGS )
endif()


include_directories(
	${CMAKE_SOURCE_DIR}/leveldb/include
	${CMAKE_SOURCE_DIR}/snappy/
	${CMAKE_SOURCE_DIR}/libspatialindex/include
	${CMAKE_SOURCE_DIR}/ZFXMath/include
	${CMAKE_SOURCE_DIR}/protobuf/src
)


add_subdirectory(protobuf/cmake EXCLUDE_FROM_ALL )
add_subdirectory(leveldb EXCLUDE_FROM_ALL)
add_subdirectory(snappy EXCLUDE_FROM_ALL)
add_subdirectory(libspatialindex EXCLUDE_FROM_ALL)

set  (THIRDPARTY
      leveldb
      snappy
	  spatialindex-64
	  libprotobuf-lite)

set_property(GLOBAL PROPERTY USE_FOLDERS ON)
set_property(TARGET ${THIRDPARTY} PROPERTY FOLDER "3rdparty")

link_libraries (
	${THIRDPARTY}
)


add_executable (
   o5mindexer
   ${O5MSOURCES}
)

if(CMAKE_CONFIGURATION_TYPES)
	set(CMAKE_CONFIGURATION_TYPES Debug Release RelWithDebInfo)
	set(CMAKE_CONFIGURATION_TYPES "${CMAKE_CONFIGURATION_TYPES}" CACHE STRING
		"Reset the configurations to what we need"
		FORCE)
endif()
