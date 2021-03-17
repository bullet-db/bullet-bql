/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.BetweenPredicateNode;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.SubFieldExpressionNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.CastExpression;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.ListExpression;
import com.yahoo.bullet.query.expressions.NAryExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.UnaryExpression;
import com.yahoo.bullet.typesystem.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TypeChecker {
    static Optional<List<BulletError>> validateSubFieldType(SubFieldExpressionNode node, FieldExpression subFieldExpression, FieldExpression fieldExpression) {
        Type type = fieldExpression.getType();
        if (Type.isUnknown(type)) {
            return unknownError();
        } else if (!isCollection(type) || (Type.isList(type) && node.getIndex() == null && node.getExpressionKey() == null) || (Type.isMap(type) && node.getIndex() != null)) {
            return makeError(node, QueryError.SUBFIELD_INVALID_DUE_TO_FIELD_TYPE, node, node.getField(), type);
        }
        if (node.getExpressionKey() != null) {
            if (subFieldExpression.getVariableSubKey() != null) {
                Type keyType = subFieldExpression.getVariableSubKey().getType();
                if (!Type.isUnknown(keyType) && keyType != Type.STRING) {
                    return makeError(node, QueryError.SUBFIELD_SUB_KEY_INVALID_TYPE, node, keyType);
                }
            } else {
                Type keyType = subFieldExpression.getVariableKey().getType();
                if (Type.isUnknown(keyType)) {
                    return Optional.empty();
                }
                if (Type.isList(type)) {
                    if (keyType != Type.INTEGER && keyType != Type.LONG) {
                        return makeError(node, QueryError.SUBFIELD_INDEX_INVALID_TYPE, node, keyType);
                    }
                } else if (keyType != Type.STRING) {
                    return makeError(node, QueryError.SUBFIELD_KEY_INVALID_TYPE, node, keyType);
                }
            }
        }
        return Optional.empty();
    }

    static Optional<List<BulletError>> validateListSubTypes(ListExpressionNode node, ListExpression listExpression) {
        Set<Type> listSubTypes = listExpression.getValues().stream().map(Expression::getType).collect(Collectors.toSet());
        if (listSubTypes.isEmpty()) {
            return makeError(node, QueryError.EMPTY_LISTS_NOT_SUPPORTED);
        }
        if (listSubTypes.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        if (listSubTypes.size() > 1) {
            return makeError(node, QueryError.LIST_HAS_MULTIPLE_TYPES, node, listSubTypes);
        }
        Type subType = listSubTypes.iterator().next();
        if (!Type.isPrimitive(subType) && !Type.isPrimitiveMap(subType)) {
            return makeError(node, QueryError.LIST_HAS_INVALID_SUBTYPE, node, subType);
        }
        return Optional.empty();
    }

    static Optional<List<BulletError>> validateBetweenType(BetweenPredicateNode node, Expression value, Expression lower, Expression upper) {
        Type valueType = value.getType();
        Type lowerType = lower.getType();
        Type upperType = upper.getType();
        if (Type.isUnknown(valueType) || Type.isUnknown(lowerType) || Type.isUnknown(upperType)) {
            return unknownError();
        }
        List<BulletError> errors = new ArrayList<>();
        if (!Type.isNumeric(valueType)) {
            errors.add(makeErrorOnly(node, QueryError.BETWEEN_VALUE_NOT_NUMERIC, node, valueType));
        }
        if (!Type.isNumeric(lowerType)) {
            errors.add(makeErrorOnly(node, QueryError.BETWEEN_BEGIN_NOT_NUMERIC, node, lowerType));
        }
        if (!Type.isNumeric(upperType)) {
            errors.add(makeErrorOnly(node, QueryError.BETWEEN_END_NOT_NUMERIC, node, upperType));
        }
        return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();

    }

    static Optional<List<BulletError>> validateUnaryType(ExpressionNode node, UnaryExpression unaryExpression) {
        Type operandType = unaryExpression.getOperand().getType();
        if (Type.isUnknown(operandType)) {
            return unknownError();
        }
        switch (unaryExpression.getOp()) {
            case NOT:
                if (!Type.isNumeric(operandType) && operandType != Type.BOOLEAN) {
                    return makeError(node, QueryError.NOT_HAS_WRONG_TYPE, node, operandType);
                }
                return Optional.empty();
            case SIZE_OF:
                if (!isCollection(operandType) && operandType != Type.STRING) {
                    return makeError(node, QueryError.SIZE_OF_HAS_WRONG_TYPE, node, operandType);
                }
                return Optional.empty();
            case IS_NULL:
            case IS_NOT_NULL:
                return Optional.empty();
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported unary operation: " + unaryExpression.getOp());
    }

    static Optional<List<BulletError>> validateNAryType(NAryExpressionNode node, NAryExpression nAryExpression) {
        List<Type> argTypes = nAryExpression.getOperands().stream().map(Expression::getType).collect(Collectors.toList());
        if (argTypes.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        // only IF is supported at the moment
        if (nAryExpression.getOp() == Operation.IF) {
            List<BulletError> errors = new ArrayList<>();
            if (argTypes.get(0) != Type.BOOLEAN) {
                errors.add(makeErrorOnly(node, QueryError.IF_FIRST_ARGUMENT_HAS_WRONG_TYPE, node, argTypes.get(0)));
            }
            if (argTypes.get(1) != argTypes.get(2)) {
                errors.add(makeErrorOnly(node, QueryError.IF_ARGUMENT_TYPES_DO_NOT_MATCH, node, argTypes.get(1), argTypes.get(2)));
            }
            return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported n-ary operation: " + nAryExpression.getOp());
    }

    static Optional<List<BulletError>> validateNumericType(ExpressionNode node, Expression expression) {
        Type type = expression.getType();
        if (Type.isUnknown(type)) {
            return unknownError();
        }
        if (!Type.isNumeric(type)) {
            return makeError(node, QueryError.EXPECTED_NUMERIC_TYPE, node, type);
        }
        return Optional.empty();
    }

    static Optional<List<BulletError>> validatePrimitiveTypes(ExpressionNode node, List<Expression> expressions) {
        List<Type> types = expressions.stream().map(Expression::getType).collect(Collectors.toList());
        if (types.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        if (!types.stream().allMatch(Type::isPrimitive)) {
            return makeError(node, QueryError.EXPECTED_PRIMITIVE_TYPES, node, types);
        }
        return Optional.empty();
    }

    static Optional<List<BulletError>> validateCastType(CastExpressionNode node, CastExpression castExpression) {
        Type argType = castExpression.getValue().getType();
        Type castType = castExpression.getCastType();
        if (Type.isUnknown(argType)) {
            return unknownError();
        } else if (!Type.canForceCast(castType, argType)) {
            return makeError(node, QueryError.CANNOT_FORCE_CAST, node.getExpression(), argType, castType);
        }
        return Optional.empty();
    }

    static Optional<List<BulletError>> validateBinaryType(BinaryExpressionNode node, BinaryExpression binaryExpression) {
        Type leftType = binaryExpression.getLeft().getType();
        Type rightType = binaryExpression.getRight().getType();
        if (Type.isUnknown(leftType) || Type.isUnknown(rightType)) {
            return unknownError();
        }
        List<BulletError> errors = new ArrayList<>();
        Type subType;
        switch (binaryExpression.getOp()) {
            case ADD:
            case SUB:
            case MUL:
            case DIV:
                if (!Type.isNumeric(leftType) || !Type.isNumeric(rightType)) {
                    return makeError(node, QueryError.BINARY_TYPES_NOT_NUMERIC, node, leftType, rightType);
                }
                return Optional.empty();
            case EQUALS:
            case NOT_EQUALS:
                if (!Type.canCompare(leftType, rightType)) {
                    return makeError(node, QueryError.BINARY_TYPES_NOT_COMPARABLE, node, leftType, rightType);
                }
                return Optional.empty();
            case EQUALS_ANY:
            case EQUALS_ALL:
            case NOT_EQUALS_ANY:
            case NOT_EQUALS_ALL:
                if (!Type.isList(rightType)) {
                    return makeError(node, QueryError.BINARY_RHS_NOT_A_LIST, node, rightType);
                }
                if (!Type.canCompare(leftType, rightType.getSubType())) {
                    return makeError(node, QueryError.BINARY_LHS_AND_SUBTYPE_OF_RHS_NOT_COMPARABLE, node, leftType, rightType);
                }
                return Optional.empty();
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
                if (!Type.isNumeric(leftType)) {
                    errors.add(makeErrorOnly(node, QueryError.BINARY_LHS_NOT_NUMERIC, node, leftType));
                }
                if (!Type.isNumeric(rightType)) {
                    errors.add(makeErrorOnly(node, QueryError.BINARY_RHS_NOT_NUMERIC, node, rightType));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
            case GREATER_THAN_ANY:
            case GREATER_THAN_ALL:
            case GREATER_THAN_OR_EQUALS_ANY:
            case GREATER_THAN_OR_EQUALS_ALL:
            case LESS_THAN_ANY:
            case LESS_THAN_ALL:
            case LESS_THAN_OR_EQUALS_ANY:
            case LESS_THAN_OR_EQUALS_ALL:
                if (!Type.isNumeric(leftType)) {
                    errors.add(makeErrorOnly(node, QueryError.BINARY_LHS_NOT_NUMERIC, node, leftType));
                }
                if (!Type.isPrimitiveList(rightType) || !Type.isNumeric(rightType.getSubType())) {
                    errors.add(makeErrorOnly(node, QueryError.BINARY_RHS_NOT_NUMERIC_LIST, node, rightType));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
            case REGEX_LIKE:
            case NOT_REGEX_LIKE:
                if (leftType != Type.STRING || rightType != Type.STRING) {
                    return makeError(node, QueryError.BINARY_TYPES_NOT_STRING, node, leftType, rightType);
                }
                return Optional.empty();
            case REGEX_LIKE_ANY:
            case NOT_REGEX_LIKE_ANY:
                if (leftType != Type.STRING) {
                    errors.add(makeErrorOnly(node, QueryError.BINARY_LHS_NOT_STRING, node, leftType));
                }
                if (rightType != Type.STRING_LIST) {
                    errors.add(makeErrorOnly(node, QueryError.BINARY_RHS_NOT_STRING_LIST, node, rightType));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
            case SIZE_IS:
                if (!isCollection(leftType) && leftType != Type.STRING) {
                    errors.add(makeErrorOnly(node, QueryError.SIZE_IS_HAS_WRONG_TYPE, node, leftType));
                }
                if (!Type.isNumeric(rightType)) {
                    errors.add(makeErrorOnly(node, QueryError.SIZE_IS_NOT_NUMERIC, node, rightType));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
            case CONTAINS_KEY:
                if (!Type.isMap(leftType) && !Type.isComplexList(leftType)) {
                    errors.add(makeErrorOnly(node, QueryError.CONTAINS_KEY_HAS_WRONG_TYPE, node, leftType));
                }
                if (rightType != Type.STRING) {
                    errors.add(makeErrorOnly(node, QueryError.CONTAINS_KEY_NOT_STRING, node, rightType));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
            case CONTAINS_VALUE:
                if (!isCollection(leftType)) {
                    errors.add(makeErrorOnly(node, QueryError.CONTAINS_VALUE_HAS_WRONG_TYPE, node, leftType));
                }
                if (!Type.isPrimitive(rightType)) {
                    errors.add(makeErrorOnly(node, QueryError.CONTAINS_VALUE_NOT_PRIMITIVE, node, rightType));
                }
                if (!errors.isEmpty()) {
                    return Optional.of(errors);
                }
                subType = leftType.getSubType();
                if (subType != rightType && subType.getSubType() != rightType) {
                    return makeError(node, QueryError.CONTAINS_VALUE_PRIMITIVES_DO_NOT_MATCH, node, leftType, rightType);
                }
                return Optional.empty();
            case IN:
            case NOT_IN:
                if (!Type.isPrimitive(leftType)) {
                    errors.add(makeErrorOnly(node, QueryError.BINARY_LHS_NOT_PRIMITIVE, node, leftType));
                }
                if (!isCollection(rightType)) {
                    errors.add(makeErrorOnly(node, QueryError.BINARY_RHS_NOT_LIST_OR_MAP, node, rightType));
                }
                if (!errors.isEmpty()) {
                    return Optional.of(errors);
                }
                subType = rightType.getSubType();
                if (subType != leftType && subType.getSubType() != leftType) {
                    return makeError(node, QueryError.IN_PRIMITIVES_NOT_MATCHING, node, leftType, rightType);
                }
                return Optional.empty();
            case AND:
            case OR:
            case XOR:
                if (leftType != Type.BOOLEAN || rightType != Type.BOOLEAN) {
                    return makeError(node, QueryError.EXPECTED_BOOLEAN_TYPES, node, leftType, rightType);
                }
                return Optional.empty();
            case FILTER:
                if (!Type.isList(leftType)) {
                    errors.add(makeErrorOnly(node, QueryError.FILTER_NOT_LIST, node, leftType));
                }
                if (rightType != Type.BOOLEAN_LIST) {
                    errors.add(makeErrorOnly(node, QueryError.FILTER_NOT_BOOLEAN_LIST, node, rightType));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported binary operation: " + binaryExpression.getOp());
    }

    // This is a static method and not a constant because a static final Optional is semantically inappropriate
    private static Optional<List<BulletError>> unknownError() {
        return Optional.of(Collections.emptyList());
    }

    private static BulletError makeErrorOnly(Node node, QueryError error) {
        return error.format(node.getLocation());
    }

    private static BulletError makeErrorOnly(Node node, QueryError error, Object... arguments) {
        return error.format(node.getLocation(), arguments);
    }

    private static Optional<List<BulletError>> makeError(BulletError error) {
        return Optional.of(Collections.singletonList(error));
    }

    private static Optional<List<BulletError>> makeError(Node node, QueryError error) {
        return makeError(makeErrorOnly(node, error));
    }

    private static Optional<List<BulletError>> makeError(Node node, QueryError error, Object... arguments) {
        return makeError(makeErrorOnly(node, error, arguments));
    }

    private static boolean isCollection(Type type) {
        return Type.isList(type) || Type.isMap(type);
    }
}
