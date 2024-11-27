#pragma once

#include "duckdb_extension.h"

duckdb_state RegisterExtraInfoScalarFunction(duckdb_connection connection);
duckdb_state RegisterScalarFunctionSetFunction(duckdb_connection connection);
duckdb_state RegisterVariadicAnyScalarFunction(duckdb_connection connection);