#include "duckdb_extension.h"

DUCKDB_EXTENSION_EXTERN

static void LibraryVersionFunction(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	const char *version = duckdb_library_version();

	idx_t input_size = duckdb_data_chunk_get_size(input);

	for (idx_t row = 0; row < input_size; row++) {
		duckdb_vector_assign_string_element(output, row, version);
	}
}

void RegisterLibraryVersionFunction(duckdb_connection connection) {
	duckdb_scalar_function function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "capi_test_duckdb_library_version");

	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
	duckdb_scalar_function_set_return_type(function, type);
	duckdb_destroy_logical_type(&type);

	duckdb_scalar_function_set_function(function, LibraryVersionFunction);

	duckdb_register_scalar_function(connection, function);
	duckdb_destroy_scalar_function(&function);
}
