#include "duckdb_extension.h"

#include "functions/scalar/query_library_function.h"

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, struct duckdb_extension_access *access) {
	RegisterLibraryVersionFunction(connection);


	// Return true to indicate succesful initialization
	return true;
}
