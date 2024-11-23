add_library(
        scalar_functions
        OBJECT
        query_library_version.c
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:scalar_functions>
        PARENT_SCOPE)
