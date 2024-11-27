#include "duckdb_extension.h"
#include <stdlib.h>

DUCKDB_EXTENSION_EXTERN

struct my_bind_data_struct {
    int64_t size;
};

void my_bind(duckdb_bind_info info) {
    if (duckdb_bind_get_parameter_count(info) != 1) {
        return;
    }

    duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    duckdb_bind_add_result_column(info, "forty_two", type);
    duckdb_destroy_logical_type(&type);

   struct my_bind_data_struct *my_bind_data = malloc(sizeof(struct my_bind_data_struct));
    duckdb_value param = duckdb_bind_get_parameter(info, 0);
    my_bind_data->size = duckdb_get_int64(param);
    duckdb_destroy_value(&param);

    duckdb_bind_set_bind_data(info, my_bind_data, free);
}

void my_error_bind(duckdb_bind_info info) {
    duckdb_bind_set_error(info, "my bind error message");
}

struct my_init_data_struct {
    int64_t pos;
};

void my_init(duckdb_init_info info) {
    if (duckdb_init_get_bind_data(info) == NULL) {
        return;
    }

    struct my_init_data_struct *my_init_data = malloc(sizeof(struct my_init_data_struct));
    my_init_data->pos = 0;
    duckdb_init_set_init_data(info, my_init_data, free);
}

void my_error_init(duckdb_init_info info) {
    duckdb_init_set_error(info, "my init error message");
}

void my_function(duckdb_function_info info, duckdb_data_chunk output) {
    struct my_bind_data_struct *bind_data = duckdb_function_get_bind_data(info);
    struct my_init_data_struct *init_data = duckdb_function_get_init_data(info);
    duckdb_vector output_vector = duckdb_data_chunk_get_vector(output, 0);
    int64_t *ptr = duckdb_vector_get_data(output_vector);

    idx_t vector_size = duckdb_vector_size();
    idx_t i;
    for (i = 0; i < vector_size; i++) {
        if (init_data->pos >= bind_data->size) {
            break;
        }
        ptr[i] = init_data->pos % 2 == 0 ? 42 : 84;
        init_data->pos++;
    }
    duckdb_data_chunk_set_size(output, i);
}

void my_error_function(duckdb_function_info info, duckdb_data_chunk output) {
    duckdb_function_set_error(info, "my function error message");
}

duckdb_state getTableFunction(duckdb_connection connection, const char *name,
        duckdb_table_function_bind_t bind,
        duckdb_table_function_init_t init,
        duckdb_table_function_t func) {

    duckdb_table_function function = duckdb_create_table_function();
    duckdb_table_function_set_name(function, name);

    duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    duckdb_table_function_add_parameter(function, type);
    duckdb_destroy_logical_type(&type);

    duckdb_logical_type named_type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    duckdb_table_function_add_named_parameter(function, "my_parameter", named_type);
    duckdb_destroy_logical_type(&named_type);

    duckdb_table_function_set_bind(function, bind);
    duckdb_table_function_set_init(function, init);
    duckdb_table_function_set_function(function, func);

    duckdb_state state = duckdb_register_table_function(connection, function);
    duckdb_destroy_table_function(&function);
    return state;
}

duckdb_state RegisterErrorTableFunctions(duckdb_connection connection) {
    duckdb_state state = getTableFunction(connection, "capi_bind_error_table_function",
            my_error_bind, my_init, my_function);
    if (state == DuckDBError) {
        return state;
    }

    state = getTableFunction(connection, "capi_init_error_table_function",
            my_bind, my_error_init, my_function);
    if (state == DuckDBError) {
        return state;
    }

    state = getTableFunction(connection, "capi_function_error_table_function",
            my_bind, my_init, my_error_function);
    return state;
}