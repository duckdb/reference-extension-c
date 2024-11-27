#include "duckdb_extension.h"
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

// FIXME: strcpy causes a warning on Windows. C99 does not have strcpy_s.

void AppendToPrefix(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    const char *extra_info = (const char *)duckdb_scalar_function_get_extra_info(info);
    idx_t extra_info_len = 7;

    // Set up input.
    idx_t input_size = duckdb_data_chunk_get_size(input);
    duckdb_vector input_vector = duckdb_data_chunk_get_vector(input, 0);
	duckdb_string_t *input_data = (duckdb_string_t *)duckdb_vector_get_data(input_vector);

    // NOTE: For simplicity, we assume there are no NULL values.
    // See CountNULL for NULL value handling.
    for (idx_t row = 0; row < input_size; row++) {
        duckdb_string_t input_char = input_data[row];

        // Determine the result string length.
        idx_t result_len = extra_info_len;
        bool is_inlined = duckdb_string_is_inlined(input_char);
        if (is_inlined) {
            result_len += input_char.value.inlined.length;
        } else {
            result_len += input_char.value.pointer.length;
        }

        // Create the result string.
        char *result = (char *)malloc(result_len);
        strcpy(result, extra_info);
        if (is_inlined) {
            strcpy(result + extra_info_len, input_char.value.inlined.inlined);
        } else {
            strcpy(result + extra_info_len, input_char.value.pointer.ptr);
        }

        // Assign and free.
        duckdb_vector_assign_string_element_len(output, row, result, result_len);
        free(result);
    }
}

duckdb_state RegisterExtraInfoScalarFunction(duckdb_connection connection) {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "capi_extra_info_scalar_function");

    char *prefix = (char *)malloc(8 * sizeof(char));
    strcpy(prefix, "prefix_");

    duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_scalar_function_add_parameter(function, type);
    duckdb_scalar_function_set_return_type(function, type);
    duckdb_destroy_logical_type(&type);

    duckdb_scalar_function_set_function(function, AppendToPrefix);
    duckdb_scalar_function_set_extra_info(function, (duckdb_function_info)prefix, free);

    duckdb_state state = duckdb_register_scalar_function(connection, function);
    duckdb_destroy_scalar_function(&function);
    return state;
}

void VariadicAddition(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t input_size = duckdb_data_chunk_get_size(input);
    idx_t column_count = duckdb_data_chunk_get_column_count(input);

    int64_t **data_ptrs = (int64_t **)malloc(column_count * sizeof(int64_t *));
    for (idx_t i = 0; i < column_count; i++) {
        duckdb_vector col = duckdb_data_chunk_get_vector(input, i);
        data_ptrs[i] = (int64_t *)duckdb_vector_get_data(col);
    }

    // NOTE: For simplicity, we assume there are no NULL values.
    // See CountNULL for NULL value handling.
    int64_t *result_data = (int64_t *)duckdb_vector_get_data(output);
    for (idx_t row_idx = 0; row_idx < input_size; row_idx++) {
        result_data[row_idx] = 0;
        for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
            int64_t data = data_ptrs[col_idx][row_idx];
            result_data[row_idx] += data;
        }
    }
    free(data_ptrs);
}

duckdb_scalar_function GetVariadicAdditionScalarFunction(duckdb_connection connection, const char *name, idx_t parameter_count) {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, name);

    duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
    for (idx_t idx = 0; idx < parameter_count; idx++) {
        duckdb_scalar_function_add_parameter(function, type);
    }
    duckdb_scalar_function_set_return_type(function, type);
    duckdb_destroy_logical_type(&type);

    duckdb_scalar_function_set_function(function, VariadicAddition);
    return function;
}

duckdb_state RegisterScalarFunctionSetFunction(duckdb_connection connection) {
    duckdb_scalar_function_set function_set = duckdb_create_scalar_function_set("variadic_addition_set");

    duckdb_scalar_function function = GetVariadicAdditionScalarFunction(connection, "capi_add_two_scalar_function", 2);
    duckdb_add_scalar_function_to_set(function_set, function);
    duckdb_destroy_scalar_function(&function);

    function = GetVariadicAdditionScalarFunction(connection, "capi_add_three_scalar_function", 3);
    duckdb_add_scalar_function_to_set(function_set, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_state state = duckdb_register_scalar_function_set(connection, function_set);
    duckdb_destroy_scalar_function_set(&function_set);
    return state;
}

void CountNULL(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
    idx_t input_size = duckdb_data_chunk_get_size(input);
    idx_t column_count = duckdb_data_chunk_get_column_count(input);

    // Extra shenanigans to test duckdb_scalar_function_set_error.
    if (column_count == 0) {
        duckdb_scalar_function_set_error(info, "please provide at least one input parameter");
        return;
    }

    // Extract the validity masks.
    uint64_t **validity_masks = (uint64_t **)malloc(column_count * sizeof(uint64_t *));
    for (idx_t i = 0; i < column_count; i++) {
        duckdb_vector col = duckdb_data_chunk_get_vector(input, i);
        validity_masks[i] = (uint64_t *)duckdb_vector_get_validity(col);
    }

    uint64_t *result_data = duckdb_vector_get_data(output);
    for (idx_t row_idx = 0; row_idx < input_size; row_idx++) {
        idx_t null_count = 0;
        for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
            if (!duckdb_validity_row_is_valid(validity_masks[col_idx], row_idx)) {
                null_count++;
            }
        }
        result_data[row_idx] = null_count;
    }
    free(validity_masks);
}

duckdb_state RegisterVariadicAnyScalarFunction(duckdb_connection connection) {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "capi_variadic_any_scalar_function");

    duckdb_logical_type any_type = duckdb_create_logical_type(DUCKDB_TYPE_ANY);
    duckdb_scalar_function_set_varargs(function, any_type);
    duckdb_destroy_logical_type(&any_type);

    duckdb_scalar_function_set_special_handling(function);
    duckdb_scalar_function_set_volatile(function);

    duckdb_logical_type return_type = duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
    duckdb_scalar_function_set_return_type(function, return_type);
    duckdb_destroy_logical_type(&return_type);

    duckdb_scalar_function_set_function(function, CountNULL);

    duckdb_state state = duckdb_register_scalar_function(connection, function);
    duckdb_destroy_scalar_function(&function);
    return state;
}