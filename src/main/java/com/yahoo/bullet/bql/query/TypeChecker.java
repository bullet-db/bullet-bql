/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.expressions.Operation;
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
            return makeError("The field " + node.getField().getValue() + " does not exist in the schema.");
        } else if (hasSubKey) {
            return !isComplex(type) ? makeError("The subfield " + node + " is invalid since the field " + node.getField().getValue() + " has type: " + type) : Optional.empty();
        } else if (hasIndexOrKey) {
            return !isCollection(type) ? makeError("The subfield " + node + " is invalid since the field " + node.getField().getValue() + " has type: " + type) : Optional.empty();
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
            return makeError("Empty lists are currently not supported.");
        }
        if (types.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        if (types.size() > 1) {
            return makeError("The list " + node + " consists of objects of multiple types: " + types);
        }
        Type subType = types.iterator().next();
        if (!Type.isPrimitive(subType) && !Type.isPrimitiveMap(subType)) {
            return makeError("The list " + node + " must consist of objects of a single primitive or primitive map type. Subtype given: " + subType);
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
                return !Type.isNumeric(type) && type != Type.BOOLEAN ? makeError("The type of the argument in " + node + " must be numeric or BOOLEAN. Type given: " + type) : Optional.empty();
            case SIZE_OF:
                return !isCollection(type) && type != Type.STRING ? makeError("The type of the argument in " + node + " must be some LIST, MAP, or STRING. Type given: " + type) : Optional.empty();
            case IS_NULL:
            case IS_NOT_NULL:
                return Optional.empty();
        }
        // Unreachable normally
        return makeError("This is not a unary operation: " + op);
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
        return Type.UNKNOWN;
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
                    errors.add(new BulletError("The type of the first argument in " + node + " must be BOOLEAN. Type given: " + types.get(0), null));
                }
                if (types.get(1) != types.get(2)) {
                    errors.add(new BulletError("The types of the second and third arguments in " + node + " must match. Types given: " + types.get(1) + ", " + types.get(2), null));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
        }
        // Unreachable normally
        return makeError("This is not a supported n-ary operation: " + op);
    }

    public static Type getNAryType(List<Type> types, Operation op) {
        // only IF is supported at the moment
        switch (op) {
            case IF:
                return types.get(1);
        }
        // Unreachable normally
        return Type.UNKNOWN;
    }

    public static Optional<List<BulletError>> validateNumericType(ExpressionNode node, Type type) {
        if (Type.isUnknown(type)) {
            return unknownError();
        }
        if (!Type.isNumeric(type)) {
            return makeError("The type of the argument in " + node + " must be numeric. Type given: " + type);
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
        return Type.UNKNOWN;
    }

    public static Optional<List<BulletError>> validateKnownTypes(List<Type> types) {
        if (types.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validateCastType(CastExpressionNode node, Type type, Type castType) {
        if (Type.isUnknown(type)) {
            return unknownError();
        } else if (!Type.canForceCast(castType, type)) {
            return makeError("Cannot cast " + node.getExpression() + " from " + type + " to " + castType + ".");
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
                return !Type.isNumeric(leftType) || !Type.isNumeric(rightType) ? makeError("The left and right operands in " + node + " must be numbers. Types given: " + leftType + ", " + rightType) : Optional.empty();
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
                if (!Type.isNumeric(leftType)) {
                    errors.add(new BulletError("The left operand in " + node + " must be numeric. Type given: " + leftType, null));
                }
                if (node.getModifier() != null) {
                    if (!Type.isPrimitiveList(rightType) || !Type.isNumeric(rightType.getSubType())) {
                        errors.add(new BulletError("The right operand in " + node + " must be some numeric LIST. Type given: " + rightType, null));
                    }
                    return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
                }
                if (!Type.isNumeric(rightType)) {
                    errors.add(new BulletError("The right operand in " + node + " must be numeric. Type given: " + rightType, null));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
            case EQUALS:
            case NOT_EQUALS:
                if (node.getModifier() != null) {
                    if (!Type.isList(rightType)) {
                        return makeError("The right operand in " + node + " must be some LIST. Type given: " + rightType);
                    }
                    if (Type.isNumeric(leftType) && Type.isNumeric(rightType.getSubType())) {
                        return Optional.empty();
                    }
                    return leftType != rightType.getSubType() ? makeError("The type of the left operand and the subtype of the right operand in " + node + " must be comparable or the same. Types given: " + leftType + ", " + rightType) : Optional.empty();
                }
                if (Type.isNumeric(leftType) && Type.isNumeric(rightType)) {
                    return Optional.empty();
                }
                return leftType != rightType ? makeError("The left and right operands in " + node + " must be comparable or have the same type. Types given: " + leftType + ", " + rightType) : Optional.empty();
            case REGEX_LIKE:
                return leftType != Type.STRING || rightType != Type.STRING ? makeError("The types of the arguments in " + node + " must be STRING. Types given: " + leftType + ", " + rightType) : Optional.empty();
            case SIZE_IS:
                if (!isCollection(leftType) && leftType != Type.STRING) {
                    errors.add(new BulletError("The type of the first argument in " + node + " must be some LIST, MAP, or STRING. Type given: " + leftType, null));
                }
                if (!Type.isNumeric(rightType)) {
                    errors.add(new BulletError("The type of the second argument in " + node + " must be numeric. Type given: " + rightType, null));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
            case CONTAINS_KEY:
                if (!Type.isMap(leftType) && !Type.isComplexList(leftType)) {
                    errors.add(new BulletError("The type of the first argument in " + node + " must be some MAP or MAP_LIST. Type given: " + leftType, null));
                }
                if (rightType != Type.STRING) {
                    errors.add(new BulletError("The type of the second argument in " + node + " must be STRING. Type given: " + rightType, null));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
            case CONTAINS_VALUE:
                if (!isCollection(leftType)) {
                    errors.add(new BulletError("The type of the first argument in " + node + " must be some LIST or MAP. Type given: " + leftType, null));
                }
                if (!Type.isPrimitive(rightType)) {
                    errors.add(new BulletError("The type of the second argument in " + node + " must be primitive. Type given: " + rightType, null));
                }
                if (!errors.isEmpty()) {
                    return Optional.of(errors);
                }
                subType = leftType.getSubType();
                return subType != rightType && subType.getSubType() != rightType ?
                       makeError("The primitive type of the first argument and the type of the second argument in " + node + " must match. Types given: " + leftType + ", " + rightType) :
                       Optional.empty();
            case IN:
                if (!Type.isPrimitive(leftType)) {
                    errors.add(new BulletError("The type of the left operand in " + node + " must be primitive. Type given: " + leftType, null));
                }
                if (!isCollection(rightType)) {
                    errors.add(new BulletError("The type of the right operand in " + node + " must be some LIST or MAP. Type given: " + rightType, null));
                }
                if (!errors.isEmpty()) {
                    return Optional.of(errors);
                }
                subType = rightType.getSubType();
                return subType != leftType && subType.getSubType() != leftType ?
                       makeError("The type of the left operand and the primitive type of the right operand in " + node + " must match. Types given: " + leftType + ", " + rightType) :
                       Optional.empty();
            case AND:
            case OR:
            case XOR:
                return leftType != Type.BOOLEAN || rightType != Type.BOOLEAN ? makeError("The types of the arguments in " + node + " must be BOOLEAN. Types given: " + leftType + ", " + rightType) : Optional.empty();
            case FILTER:
                if (!Type.isList(leftType)) {
                    errors.add(new BulletError("The type of the first argument in " + node + " must be some LIST. Type given: " + leftType, null));
                }
                if (rightType != Type.BOOLEAN_LIST) {
                    errors.add(new BulletError("The type of the second argument in " + node + " must be BOOLEAN_LIST. Type given: " + rightType, null));
                }
                return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
        }
        // Unreachable normally
        return makeError("This is not a binary operation: " + op);
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
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
            case REGEX_LIKE:
            case SIZE_IS:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
            case IN:
            case AND:
            case OR:
            case XOR:
                return Type.BOOLEAN;
            case FILTER:
                return leftType;
        }
        // Unreachable normally
        return Type.UNKNOWN;
    }

    // This is a static method and not a constant because a static final Optional is semantically inappropriate
    private static Optional<List<BulletError>> unknownError() {
        return Optional.of(Collections.emptyList());
    }

    private static Optional<List<BulletError>> makeError(String message) {
        return Optional.of(Collections.singletonList(new BulletError(message, null)));
    }

    private static boolean isComplex(Type type) {
        return Type.isComplexList(type) || Type.isComplexMap(type);
    }

    private static boolean isCollection(Type type) {
        return Type.isList(type) || Type.isMap(type);
    }
}
