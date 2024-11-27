#include "duckdb_extension.h"

#include "functions/query_library_version.h"
#include "functions/query_scalar_functions.h"
#include "functions/query_table_functions.h"

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection, duckdb_extension_info info, struct duckdb_extension_access *access) {
    // Library version.
    if (RegisterLibraryVersionFunction(connection) != DuckDBSuccess) {
        return false;
    }

    // Scalar functions.
    if (RegisterExtraInfoScalarFunction(connection) != DuckDBSuccess) {
        return false;
    }
    if (RegisterScalarFunctionSetFunction(connection) != DuckDBSuccess) {
        return false;
    }
    if (RegisterVariadicAnyScalarFunction(connection) != DuckDBSuccess) {
        return false;
    }

    // Table functions.
    if (RegisterErrorTableFunctions(connection) != DuckDBSuccess) {
        return false;
    }

	// Return true to indicate successful initialization.
	return true;
}
