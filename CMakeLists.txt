cmake_minimum_required(VERSION 2.8)

project (o5mindexer)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY  "${CMAKE_SOURCE_DIR}/bin")

if(CMAKE_COMPILER_IS_GNUCXX)
	#set(CMAKE_BUILD_TYPE Debug)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif()


file(GLOB_RECURSE SOURCES src/*.h src/*.c src/*.cpp)

foreach(f ${SOURCES})
    # Get the path of the file relative to ${DIRECTORY},
    # then alter it (not compulsory)
    file(RELATIVE_PATH SRCGR ${CMAKE_CURRENT_SOURCE_DIR} ${f})
    set(SRCGR "Sources/${SRCGR}")

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


add_executable (
   o5mindexer
   ${SOURCES}
)
