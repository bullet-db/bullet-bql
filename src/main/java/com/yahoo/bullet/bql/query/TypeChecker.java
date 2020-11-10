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
import com.yahoo.bullet.bql.tree.SubFieldExpressionNode;
import com.yahoo.bullet.bql.tree.SubSubFieldExpressionNode;
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
    public static Optional<List<BulletError>> validateFieldType(FieldExpressionNode node, FieldExpression fieldExpression, Type type) {
        if (Type.isUnknown(type)) {
            return unknownError();
        } else if (Type.isNull(type)) {
            return makeError(node, "The field " + fieldExpression.getField() + " does not exist in the schema.");
        }
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validateSubFieldType(SubFieldExpressionNode node, FieldExpression fieldExpression) {
        Type type = fieldExpression.getType();
        if (Type.isUnknown(type)) {
            return unknownError();
        } else if (!isCollection(type)) {
            return makeError(node, "The subfield " + node + " is invalid since the field " + node.getField() + " has type: " + type);
        }
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validateListSubTypes(ListExpressionNode node, ListExpression listExpression) {
        Set<Type> listSubTypes = listExpression.getValues().stream().map(Expression::getType).collect(Collectors.toSet());
        if (listSubTypes.isEmpty()) {
            return makeError(node, "Empty lists are currently not supported.");
        }
        if (listSubTypes.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        if (listSubTypes.size() > 1) {
            return makeError(node, "The list " + node + " consists of objects of multiple types: " + listSubTypes);
        }
        Type subType = listSubTypes.iterator().next();
        if (!Type.isPrimitive(subType) && !Type.isPrimitiveMap(subType)) {
            return makeError(node, "The list " + node + " must consist of objects of a single primitive or primitive map type. Subtype given: " + subType);
        }
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validateUnaryType(ExpressionNode node, UnaryExpression unaryExpression) {
        Type operandType = unaryExpression.getOperand().getType();
        if (Type.isUnknown(operandType)) {
            return unknownError();
        }
        switch (unaryExpression.getOp()) {
            case NOT:
                if (!Type.isNumeric(operandType) && operandType != Type.BOOLEAN) {
                    return makeError(node, "The type of the argument in " + node + " must be numeric or BOOLEAN. Type given: " + operandType);
                }
                return Optional.empty();
            case SIZE_OF:
                if (!isCollection(operandType) && operandType != Type.STRING) {
                    return makeError(node, "The type of the argument in " + node + " must be some LIST, MAP, or STRING. Type given: " + operandType);
                }
                return Optional.empty();
            case IS_NULL:
            case IS_NOT_NULL:
                return Optional.empty();
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported unary operation: " + unaryExpression.getOp());
    }

    public static Optional<List<BulletError>> validateNAryType(NAryExpressionNode node, NAryExpression nAryExpression) {
        List<Type> argTypes = nAryExpression.getOperands().stream().map(Expression::getType).collect(Collectors.toList());
        if (argTypes.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        // only IF is supported at the moment
        if (nAryExpression.getOp() == Operation.IF) {
            List<BulletError> errors = new ArrayList<>();
            if (argTypes.get(0) != Type.BOOLEAN) {
                errors.add(makeErrorOnly(node, "The type of the first argument in " + node + " must be BOOLEAN. Type given: " + argTypes.get(0)));
            }
            if (argTypes.get(1) != argTypes.get(2)) {
                errors.add(makeErrorOnly(node, "The types of the second and third arguments in " + node + " must match. Types given: " + argTypes.get(1) + ", " + argTypes.get(2)));
            }
            return !errors.isEmpty() ? Optional.of(errors) : Optional.empty();
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported n-ary operation: " + nAryExpression.getOp());
    }

    public static Optional<List<BulletError>> validateNumericType(ExpressionNode node, Expression expression) {
        Type type = expression.getType();
        if (Type.isUnknown(type)) {
            return unknownError();
        }
        if (!Type.isNumeric(type)) {
            return makeError(node, "The type of the argument in " + node + " must be numeric. Type given: " + type);
        }
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validatePrimitiveTypes(ExpressionNode node, List<Expression> expressions) {
        List<Type> types = expressions.stream().map(Expression::getType).collect(Collectors.toList());
        if (types.contains(Type.UNKNOWN)) {
            return unknownError();
        }
        if (!types.stream().allMatch(Type::isPrimitive)) {
            return makeError(node, "The types of the arguments in " + node + " must be primitive. Types given: " + types);
        }
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validateCastType(CastExpressionNode node, CastExpression castExpression) {
        Type argType = castExpression.getValue().getType();
        Type castType = castExpression.getCastType();
        if (Type.isUnknown(argType)) {
            return unknownError();
        } else if (!Type.canForceCast(castType, argType)) {
            return makeError(node, "Cannot cast " + node.getExpression() + " from " + argType + " to " + castType + ".");
        }
        return Optional.empty();
    }

    public static Optional<List<BulletError>> validateBinaryType(BinaryExpressionNode node, BinaryExpression binaryExpression) {
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
        throw new IllegalArgumentException("This is not a supported binary operation: " + binaryExpression.getOp());
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

    private static boolean isCollection(Type type) {
        return Type.isList(type) || Type.isMap(type);
    }
}
