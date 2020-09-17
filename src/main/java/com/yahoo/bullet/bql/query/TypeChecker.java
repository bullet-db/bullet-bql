/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class TypeChecker {
    public static Optional<List<BulletError>> validateFieldType(FieldExpressionNode node, Type type, boolean hasIndexOrKey, boolean hasSubKey) {
        if (Type.isUnknown(type)) {
            return unknownError();
        } else if (Type.isNull(type)) {
            return makeError(node, "The field " + node.getField().getValue() + " does not exist in the schema.");
        } else if ((hasIndexOrKey && !hasSubKey && !isCollection(type)) || (hasSubKey && !isComplex(type))) {
            return makeError(node, "The subfield " + node + " is invalid since the field " + node.getField().getValue() + " has type: " + type);
        }
        return Optional.empty();
    }

    public static Type getFieldType(Type type, boolean hasIndexOrKey, boolean hasSubKey) {
        if (hasSubKey) {
            return type.getSubType().getSubType();
        } else if (hasIndexOrKey) {
            return type.getSubType();
        }
        return type;
    }

    public static Optional<List<BulletError>> validateListTypes(ListExpressionNode node, Set<Type> types) {
        if (types.isEmpty()) {
            return makeError(node, "Empty lists are currently not supported.");
        }
        if (types.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        if (types.size() > 1) {
            return makeError(node, "The list " + node + " consists of objects of multiple types: " + types);
        }
        Type subType = types.iterator().next();
        if (!Type.isPrimitive(subType) && !Type.isPrimitiveMap(subType)) {
            return makeError(node, "The list " + node + " must consist of objects of a single primitive or primitive map type. Subtype given: " + subType);
        }
        return Optional.empty();
    }

    public static Type getListType(Set<Type> types) {
        // Assume non-empty list
        Type subType = types.iterator().next();
        return Type.LISTS.stream().filter(type -> type.getSubType().equals(subType)).findFirst().orElse(Type.UNKNOWN);
    }

    public static Optional<List<BulletError>> validateUnaryType(UnaryExpressionNode node, Type type, Operation op) {
        if (Type.isUnknown(type)) {
            return unknownError();
        }
        switch (op) {
            case NOT:
                if (!Type.isNumeric(type) && type != Type.BOOLEAN) {
                    return makeError(node, "The type of the argument in " + node + " must be numeric or BOOLEAN. Type given: " + type);
                }
                return Optional.empty();
            case SIZE_OF:
                if (!isCollection(type) && type != Type.STRING) {
                    return makeError(node, "The type of the argument in " + node + " must be some LIST, MAP, or STRING. Type given: " + type);
                }
                return Optional.empty();
            case IS_NULL:
            case IS_NOT_NULL:
                return Optional.empty();
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported unary operation: " + op);
    }

    public static Type getUnaryType(Operation op) {
        switch (op) {
            case NOT:
            case IS_NULL:
            case IS_NOT_NULL:
                return Type.BOOLEAN;
            case SIZE_OF:
                return Type.INTEGER;
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported unary operation: " + op);
    }

    public static Optional<List<BulletError>> validateNAryType(NAryExpressionNode node, List<Type> types, Operation op) {
        if (types.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        // only IF is supported at the moment
        switch (op) {
            case IF:
                List<BulletError> errors = new ArrayList<>();
                if (types.get(0) != Type.BOOLEAN) {
                    errors.add(makeErrorOnly(node, "The type of the first argument in " + node + " must be BOOLEAN. Type given: " + types.get(0)));
                }
                if (types.get(1) != types.get(2)) {
                    errors.add(makeErrorOnly(node, "The types of the second and third arguments in " + node + " must match. Types given: " + types.get(1) + ", " + types.get(2)));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported n-ary operation: " + op);
    }

    public static Type getNAryType(List<Type> types, Operation op) {
        // only IF is supported at the moment
        switch (op) {
            case IF:
                return types.get(1);
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported n-ary operation: " + op);
    }

    public static Optional<List<BulletError>> validateNumericType(ExpressionNode node, Type type) {
        if (Type.isUnknown(type)) {
            return unknownError();
        }
        if (!Type.isNumeric(type)) {
            return makeError(node, "The type of the argument in " + node + " must be numeric. Type given: " + type);
        }
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validatePrimitiveTypes(ExpressionNode node, List<Type> types) {
        if (types.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        if (!types.stream().allMatch(Type::isPrimitive)) {
            return makeError(node, "The types of the arguments in " + node + " must be primitive. Types given: " + types);
        }
        return Optional.empty();
    }

    public static Type getAggregateType(Type type, GroupOperation.GroupOperationType op) {
        switch (op) {
            case SUM:
            case MIN:
            case MAX:
                return type;
            case AVG:
                return Type.DOUBLE;
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported group operation: " + op);
    }

    public static Optional<List<BulletError>> validateCastType(CastExpressionNode node, Type type, Type castType) {
        if (Type.isUnknown(type)) {
            return unknownError();
        } else if (!Type.canForceCast(castType, type)) {
            return makeError(node, "Cannot cast " + node.getExpression() + " from " + type + " to " + castType + ".");
        }
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validateBinaryType(BinaryExpressionNode node, Type leftType, Type rightType, Operation op) {
        if (Type.isUnknown(leftType) || Type.isUnknown(rightType)) {
            return unknownError();
        }
        List<BulletError> errors = new ArrayList<>();
        Type subType;
        switch (op) {
            case ADD:
            case SUB:
            case MUL:
            case DIV:
                if (!Type.isNumeric(leftType) || !Type.isNumeric(rightType)) {
                    return makeError(node, "The left and right operands in " + node + " must be numeric. Types given: " + leftType + ", " + rightType);
                }
                return Optional.empty();
            case EQUALS:
            case NOT_EQUALS:
                if (!Type.canCompare(leftType, rightType)) {
                    return makeError(node, "The left and right operands in " + node + " must be comparable. Types given: " + leftType + ", " + rightType);
                }
                return Optional.empty();
            case EQUALS_ANY:
            case EQUALS_ALL:
            case NOT_EQUALS_ANY:
            case NOT_EQUALS_ALL:
                if (!Type.isList(rightType)) {
                    return makeError(node, "The right operand in " + node + " must be some LIST. Type given: " + rightType);
                }
                if (!Type.canCompare(leftType, rightType.getSubType())) {
                    return makeError(node, "The type of the left operand and the subtype of the right operand in " + node + " must be comparable. Types given: " + leftType + ", " + rightType);
                }
                return Optional.empty();
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
                if (!Type.isNumeric(leftType)) {
                    errors.add(makeErrorOnly(node, "The left operand in " + node + " must be numeric. Type given: " + leftType));
                }
                if (!Type.isNumeric(rightType)) {
                    errors.add(makeErrorOnly(node, "The right operand in " + node + " must be numeric. Type given: " + rightType));
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
                    errors.add(makeErrorOnly(node, "The left operand in " + node + " must be numeric. Type given: " + leftType));
                }
                if (!Type.isPrimitiveList(rightType) || !Type.isNumeric(rightType.getSubType())) {
                    errors.add(makeErrorOnly(node, "The right operand in " + node + " must be some numeric LIST. Type given: " + rightType));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
            case REGEX_LIKE:
                if (leftType != Type.STRING || rightType != Type.STRING) {
                    return makeError(node, "The types of the arguments in " + node + " must be STRING. Types given: " + leftType + ", " + rightType);
                }
                return Optional.empty();
            case REGEX_LIKE_ANY:
                if (leftType != Type.STRING) {
                    errors.add(makeErrorOnly(node, "The type of the left operand in " + node + " must be STRING. Type given: " + leftType));
                }
                if (rightType != Type.STRING_LIST) {
                    errors.add(makeErrorOnly(node, "The type of the right operand in " + node + " must be STRING_LIST. Type given: " + rightType));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
            case SIZE_IS:
                if (!isCollection(leftType) && leftType != Type.STRING) {
                    errors.add(makeErrorOnly(node, "The type of the first argument in " + node + " must be some LIST, MAP, or STRING. Type given: " + leftType));
                }
                if (!Type.isNumeric(rightType)) {
                    errors.add(makeErrorOnly(node, "The type of the second argument in " + node + " must be numeric. Type given: " + rightType));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
            case CONTAINS_KEY:
                if (!Type.isMap(leftType) && !Type.isComplexList(leftType)) {
                    errors.add(makeErrorOnly(node, "The type of the first argument in " + node + " must be some MAP or MAP_LIST. Type given: " + leftType));
                }
                if (rightType != Type.STRING) {
                    errors.add(makeErrorOnly(node, "The type of the second argument in " + node + " must be STRING. Type given: " + rightType));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
            case CONTAINS_VALUE:
                if (!isCollection(leftType)) {
                    errors.add(makeErrorOnly(node, "The type of the first argument in " + node + " must be some LIST or MAP. Type given: " + leftType));
                }
                if (!Type.isPrimitive(rightType)) {
                    errors.add(makeErrorOnly(node, "The type of the second argument in " + node + " must be primitive. Type given: " + rightType));
                }
                if (!errors.isEmpty()) {
                    return Optional.of(errors);
                }
                subType = leftType.getSubType();
                if (subType != rightType && subType.getSubType() != rightType) {
                    return makeError(node, "The primitive type of the first argument and the type of the second argument in " + node + " must match. Types given: " + leftType + ", " + rightType);
                }
                return Optional.empty();
            case IN:
            case NOT_IN:
                if (!Type.isPrimitive(leftType)) {
                    errors.add(makeErrorOnly(node, "The type of the left operand in " + node + " must be primitive. Type given: " + leftType));
                }
                if (!isCollection(rightType)) {
                    errors.add(makeErrorOnly(node, "The type of the right operand in " + node + " must be some LIST or MAP. Type given: " + rightType));
                }
                if (!errors.isEmpty()) {
                    return Optional.of(errors);
                }
                subType = rightType.getSubType();
                if (subType != leftType && subType.getSubType() != leftType) {
                    return makeError(node, "The type of the left operand and the primitive type of the right operand in " + node + " must match. Types given: " + leftType + ", " + rightType);
                }
                return Optional.empty();
            case AND:
            case OR:
            case XOR:
                if (leftType != Type.BOOLEAN || rightType != Type.BOOLEAN) {
                    return makeError(node, "The types of the arguments in " + node + " must be BOOLEAN. Types given: " + leftType + ", " + rightType);
                }
                return Optional.empty();
            case FILTER:
                if (!Type.isList(leftType)) {
                    errors.add(makeErrorOnly(node, "The type of the first argument in " + node + " must be some LIST. Type given: " + leftType));
                }
                if (rightType != Type.BOOLEAN_LIST) {
                    errors.add(makeErrorOnly(node, "The type of the second argument in " + node + " must be BOOLEAN_LIST. Type given: " + rightType));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported binary operation: " + op);
    }

    public static Type getBinaryType(Type leftType, Type rightType, Operation op) {
        switch (op) {
            case ADD:
            case SUB:
            case MUL:
            case DIV:
                if (leftType == Type.DOUBLE || rightType == Type.DOUBLE) {
                    return Type.DOUBLE;
                } else if (leftType == Type.FLOAT || rightType == Type.FLOAT) {
                    return Type.FLOAT;
                } else if (leftType == Type.LONG || rightType == Type.LONG) {
                    return Type.LONG;
                } else {
                    return Type.INTEGER;
                }
            case EQUALS:
            case EQUALS_ANY:
            case EQUALS_ALL:
            case NOT_EQUALS:
            case NOT_EQUALS_ANY:
            case NOT_EQUALS_ALL:
            case GREATER_THAN:
            case GREATER_THAN_ANY:
            case GREATER_THAN_ALL:
            case GREATER_THAN_OR_EQUALS:
            case GREATER_THAN_OR_EQUALS_ANY:
            case GREATER_THAN_OR_EQUALS_ALL:
            case LESS_THAN:
            case LESS_THAN_ANY:
            case LESS_THAN_ALL:
            case LESS_THAN_OR_EQUALS:
            case LESS_THAN_OR_EQUALS_ANY:
            case LESS_THAN_OR_EQUALS_ALL:
            case REGEX_LIKE:
            case REGEX_LIKE_ANY:
            case SIZE_IS:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
            case IN:
            case NOT_IN:
            case AND:
            case OR:
            case XOR:
                return Type.BOOLEAN;
            case FILTER:
                return leftType;
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported binary operation: " + op);
    }

    // This is a static method and not a constant because a static final Optional is semantically inappropriate
    private static Optional<List<BulletError>> unknownError() {
        return Optional.of(Collections.emptyList());
    }

    private static BulletError makeErrorOnly(Node node, String message) {
        return new BulletError(node.getLocation() + message, (List<String>) null);
    }

    private static Optional<List<BulletError>> makeError(Node node, String message) {
        return Optional.of(Collections.singletonList(makeErrorOnly(node, message)));
    }

    private static boolean isComplex(Type type) {
        return Type.isComplexList(type) || Type.isComplexMap(type);
    }

    private static boolean isCollection(Type type) {
        return Type.isList(type) || Type.isMap(type);
    }
}
