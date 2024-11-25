add_library(
        functions
        OBJECT
        query_library_version.c
        query_scalar_functions.c
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:functions>
        PARENT_SCOPE)
