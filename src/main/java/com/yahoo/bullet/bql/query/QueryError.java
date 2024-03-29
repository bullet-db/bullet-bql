/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.NodeLocation;
import com.yahoo.bullet.common.BulletError;
import lombok.AllArgsConstructor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@AllArgsConstructor
public enum QueryError {
    EMPTY_QUERY("The given BQL query is empty.", "Please specify a non-empty query."),
    QUERY_TOO_LONG("The given BQL string is too long. (%d characters)"),
    GENERIC_PARSING_ERROR("%s", "This is a parsing error."),
    GENERIC_ERROR("%s", "This is an application error and not a user error."),

    MULTIPLE_QUERY_TYPES("Query consists of multiple aggregation types.", "Please specify a valid query with only one aggregation type."),
    NESTED_AGGREGATE("Aggregates cannot be nested.", "Please remove any nested aggregates."),
    WHERE_WITH_AGGREGATE("WHERE clause cannot contain aggregates.", "If you wish to filter on an aggregate, please specify it in the HAVING clause."),
    GROUP_BY_WITH_AGGREGATE("GROUP BY clause cannot contain aggregates.", "Please remove any aggregates from the GROUP BY clause."),
    MULTIPLE_COUNT_DISTINCT("Cannot have multiple COUNT DISTINCT.", "Please specify only one COUNT DISTINCT."),
    COUNT_DISTINCT_WITH_ORDER_BY("ORDER BY clause is not supported for queries with COUNT DISTINCT.", "Please remove the ORDER BY clause."),
    COUNT_DISTINCT_WITH_LIMIT("LIMIT clause is not supported for queries with COUNT DISTINCT.", "Please remove the LIMIT clause."),
    MULTIPLE_DISTRIBUTION("Cannot have multiple distribution functions.", "Please specify only one distribution function."),
    DISTRIBUTION_AS_VALUE("Distribution functions cannot be treated as values.", Arrays.asList("Please consider using the distribution's output fields instead.",
                                                                                               "For QUANTILE distributions, the output fields are: [\"Value\", \"Quantile\"].",
                                                                                               "For FREQ and CUMFREQ distributions, the output fields are: [\"Probability\", \"Count\", \"Quantile\"].")),
    MULTIPLE_TOP_K("Cannot have multiple TOP functions.", "Please specify only one TOP function."),
    TOP_K_AS_VALUE("TOP function cannot be treated as a value.", Arrays.asList("Please consider using the TOP function's output field instead. The default name is \"Count\".",
                                                                               "The output field can also be renamed by selecting TOP with an alias.")),
    TOP_K_WITH_ORDER_BY("ORDER BY clause is not supported for queries with a TOP function.", "Please remove the ORDER BY clause."),
    TOP_K_WITH_LIMIT("LIMIT clause is not supported for queries with a TOP function.", "Please remove the LIMIT clause."),
    HAVING_WITHOUT_GROUP_BY("HAVING clause is only supported with GROUP BY clause.", "Please remove the HAVING clause, and consider using a WHERE clause instead."),
    NON_POSITIVE_DURATION("Query duration must be positive.", "Please specify a positive duration."),
    NON_POSITIVE_LIMIT("LIMIT clause must be positive.", "Please specify a positive LIMIT clause."),
    SELECT_TABLE_FUNCTION_WITH_LATERAL_VIEW("Selecting a table function is not supported in queries with a LATERAL VIEW clause.", "Please remove either the table function or the LATERAL VIEW clause."),
    SELECT_TABLE_FUNCTION_WITH_AGGREGATION("Selecting a table function is not supported with other aggregation types.", "Please consider moving the selected table function to a LATERAL VIEW clause."),
    TABLE_FUNCTION_WITH_AGGREGATE("Table functions cannot contain aggregates.", "Please consider a non-aggregate argument."),
    MULTIPLE_SELECT_TABLE_FUNCTIONS("Cannot select multiple table functions.", "Please select only one table function."),
    MULTIPLE_ALIASES("Cannot alias a field with multiple different aliases.", "Please specify only one alias."),

    WHERE_CANNOT_CAST_TO_BOOLEAN("WHERE clause cannot be casted to BOOLEAN: %s.", "Please specify a valid WHERE clause."),
    HAVING_CANNOT_CAST_TO_BOOLEAN("HAVING clause cannot be casted to BOOLEAN: %s.", "Please specify a valid HAVING clause."),
    SELECT_DISTINCT_FIELD_NON_PRIMITIVE("The SELECT DISTINCT field %s is non-primitive. Type given: %s.", "Please specify primitive fields only for SELECT DISTINCT."),
    GROUP_BY_FIELD_NON_PRIMITIVE("The GROUP BY field %s is non-primitive. Type given: %s.", "Please specify primitive fields only for GROUP BY."),
    ORDER_BY_FIELD_NON_PRIMITIVE("ORDER BY contains a non-primitive field: %s.", "Please specify a primitive field."),
    DUPLICATE_FIELD_NAMES_ALIASES("The following field names/aliases are shared: %s.", "Please specify non-overlapping field names and aliases."),
    EXPLODE_SAME_KEY_AND_VALUE_ALIASES("The key and value aliases in %s are the same.", "Please specify different aliases for the key and value."),

    // Incorrect number of arguments
    IF_INCORRECT_NUMBER_OF_ARGUMENTS("IF requires 3 arguments. The number of arguments given in %s was %s."),
    BETWEEN_INCORRECT_NUMBER_OF_ARGUMENTS("BETWEEN requires 3 arguments. The number of arguments given in %s was %s."),
    SUBSTRING_INCORRECT_NUMBER_OF_ARGUMENTS("SUBSTRING requires 2 or 3 arguments. The number of arguments given in %s was %s."),
    UNIX_TIMESTAMP_INCORRECT_NUMBER_OF_ARGUMENTS("UNIXTIMESTAMP requires 0, 1, or 2 arguments. The number of arguments given in %s was %s."),

    // Type checking
    SUBFIELD_INVALID_DUE_TO_FIELD_TYPE("The subfield %s is invalid since the field %s has type: %s."),
    SUBFIELD_INDEX_INVALID_TYPE("The type of the index in the subfield %s must be INTEGER or LONG. Type given: %s."),
    SUBFIELD_KEY_INVALID_TYPE("The type of the key in the subfield %s must be STRING. Type given: %s."),
    SUBFIELD_SUB_KEY_INVALID_TYPE("The type of the subkey in the subfield %s must be STRING. Type given: %s."),
    EMPTY_LISTS_NOT_SUPPORTED("Empty lists are currently not supported."),
    LIST_HAS_MULTIPLE_TYPES("The list %s consists of objects of multiple types: %s."),
    LIST_HAS_INVALID_SUBTYPE("The list %s must consist of objects of a single primitive or primitive map type. Subtype given: %s."),
    NOT_HAS_WRONG_TYPE("The type of the argument in %s must be numeric or BOOLEAN. Type given: %s."),
    BETWEEN_VALUES_WRONG_TYPES("The types of the values in %s must be all numeric or all STRING. Types given: %s"),
    BETWEEN_ARGS_WRONG_TYPES("The types of the arguments in %s must be all numeric or all STRING. Types given: %s"),
    SIZE_OF_HAS_WRONG_TYPE("The type of the argument in %s must be some LIST, MAP, or STRING. Type given: %s."),
    IF_FIRST_ARGUMENT_HAS_WRONG_TYPE("The type of the first argument in %s must be BOOLEAN. Type given: %s."),
    IF_ARGUMENT_TYPES_DO_NOT_MATCH("The types of the second and third arguments in %s must match. Types given: %s, %s."),
    EXPECTED_NUMERIC_OR_BOOLEAN_TYPE("The type of the argument in %s must be numeric or BOOLEAN. Type given: %s."),
    EXPECTED_PRIMITIVE_TYPES("The types of the arguments in %s must be primitive. Types given: %s."),
    CANNOT_FORCE_CAST("Cannot cast %s from %s to %s."),
    BINARY_TYPES_NOT_NUMERIC("The left and right operands in %s must be numeric. Types given: %s, %s."),
    BINARY_TYPES_NOT_NUMERIC_OR_STRING("The left and right operands in %s must either both be numeric or both be STRING. Types given: %s, %s."),
    BINARY_TYPES_NOT_COMPARABLE("The left and right operands in %s must be comparable. Types given: %s, %s."),
    BINARY_RHS_NOT_A_LIST("The right operand in %s must be some LIST. Type given: %s."),
    BINARY_LHS_AND_SUBTYPE_OF_RHS_NOT_COMPARABLE("The type of the left operand and the subtype of the right operand in %s must be comparable. Types given: %s, %s."),
    BINARY_TYPES_NOT_STRING("The types of the arguments in %s must be STRING. Types given: %s, %s."),
    BINARY_LHS_NOT_STRING("The type of the left operand in %s must be STRING. Type given: %s."),
    BINARY_RHS_NOT_STRING_LIST("The type of the right operand in %s must be STRING_LIST. Type given: %s."),
    SIZE_IS_HAS_WRONG_TYPE("The type of the first argument in %s must be some LIST, MAP, or STRING. Type given: %s."),
    SIZE_IS_NOT_NUMERIC("The type of the second argument in %s must be numeric. Type given: %s."),
    CONTAINS_KEY_HAS_WRONG_TYPE("The type of the first argument in %s must be some MAP or MAP_LIST. Type given: %s."),
    CONTAINS_KEY_NOT_STRING("The type of the second argument in %s must be STRING. Type given: %s."),
    CONTAINS_VALUE_HAS_WRONG_TYPE("The type of the first argument in %s must be some LIST or MAP. Type given: %s."),
    CONTAINS_VALUE_NOT_PRIMITIVE("The type of the second argument in %s must be primitive. Type given: %s."),
    CONTAINS_VALUE_PRIMITIVES_DO_NOT_MATCH("The primitive type of the first argument and the type of the second argument in %s must match. Types given: %s, %s."),
    BINARY_LHS_NOT_PRIMITIVE("The type of the left operand in %s must be primitive. Type given: %s."),
    BINARY_RHS_NOT_LIST_OR_MAP("The type of the right operand in %s must be some LIST or MAP. Type given: %s."),
    BINARY_LHS_NOT_NUMERIC_OR_STRING("The type of the left operand in %s must be numeric or STRING. Type given: %s."),
    BINARY_RHS_NOT_NUMERIC_OR_STRING_LIST("The type of the right operand in %s must be some numeric LIST or STRING_LIST. Type given: %s."),
    BINARY_LHS_NOT_MATCH_RHS_SUBTYPE("The type of the left operand and the subtype of the right operand in %s must either both be numeric or both be STRING. Types given: %s, %s."),
    IN_PRIMITIVES_NOT_MATCHING("The type of the left operand and the primitive type of the right operand in %s must either match or both be numeric. Types given: %s, %s."),
    EXPECTED_BOOLEAN_TYPES("The types of the arguments in %s must be BOOLEAN. Types given: %s, %s."),
    FILTER_NOT_LIST("The type of the first argument in %s must be some LIST. Type given: %s."),
    FILTER_NOT_BOOLEAN_LIST("The type of the second argument in %s must be BOOLEAN_LIST. Type given: %s."),
    ABS_HAS_WRONG_TYPE("The type of the argument in %s must be numeric. Type given: %s."),
    STRING_OP_HAS_WRONG_TYPE("The type of the argument in %s must be STRING. Type given: %s."),
    SUBSTRING_VALUE_NOT_STRING("The type of the first argument in %s must be STRING. Type given: %s."),
    SUBSTRING_START_NOT_NUMERIC("The type of the second argument (the start parameter) in %s must be numeric. Type given: %s."),
    SUBSTRING_LENGTH_NOT_NUMERIC("The type of the third argument (the length parameter) in %s must be numeric. Type given: %s."),
    UNIX_TIMESTAMP_VALUE_NOT_STRING("The type of the first argument in %s must be STRING. Type given: %s."),
    UNIX_TIMESTAMP_VALUE_NOT_STRING_OR_NUMERIC("The type of the first argument in %s must be STRING or numeric. Type given: %s."),
    UNIX_TIMESTAMP_PATTERN_NOT_STRING("The type of the second argument (the pattern parameter) in %s must be STRING. Type given: %s."),
    EXPLODE_FIELD_NOT_MAP("The type of the argument in %s must be some MAP. Type given: %s"),
    EXPLODE_FIELD_NOT_LIST("The type of the argument in %s must be some LIST. Type given: %s"),

    FIELD_NOT_IN_SCHEMA("The field %s does not exist in the schema.");

    private String errorFormat;
    private List<String> resolutions;

    QueryError(String errorFormat) {
        this(errorFormat, (List<String>) null);
    }

    QueryError(String errorFormat, String resolution) {
        this(errorFormat, Collections.singletonList(resolution));
    }

    /**
     * Formats the error as a {@link BulletError}.
     *
     * @return A {@link BulletError}.
     */
    public BulletError format() {
        return new BulletError(errorFormat, resolutions);
    }

    /**
     * Formats the error as a {@link BulletError}.
     *
     * @param arguments The non-null arguments to pass to the specific error to format itself with actual values.
     * @return A {@link BulletError}.
     */
    public BulletError format(Object... arguments) {
        return format(errorFormat, resolutions, arguments);
    }

    /**
     * Formats the error as a {@link BulletError}.
     *
     * @param location {@link NodeLocation} to prepend to the error.
     * @return A {@link BulletError}.
     */
    public BulletError format(NodeLocation location) {
        return new BulletError(location + errorFormat, resolutions);
    }

    /**
     * Formats the error as a {@link BulletError}.
     *
     * @param location {@link NodeLocation} to prepend to the error.
     * @param arguments The non-null arguments to pass to the specific error to format itself with actual values.
     * @return A {@link BulletError}.
     */
    public BulletError format(NodeLocation location, Object... arguments) {
        return format(location + errorFormat, resolutions, arguments);
    }

    /**
     * Formats the error as a {@link BulletError} with a custom resolution.
     *
     * @param resolution The dynamic resolution to add.
     * @param arguments The non-null arguments to pass to the specific error to format itself with actual values.
     * @return A {@link BulletError}.
     */
    public BulletError formatWithResolution(String resolution, Object... arguments) {
        return format(errorFormat, Collections.singletonList(resolution), arguments);
    }

    private static BulletError format(String format, List<String> resolutions, Object... arguments) {
        return new BulletError(String.format(format, arguments), resolutions);
    }
}
