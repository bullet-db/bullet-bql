/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
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
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Type;

import java.util.List;
import java.util.Optional;

public class TypeSetter {
    static void setType(FieldExpressionNode node, FieldExpression fieldExpression, LayeredSchema layeredSchema, List<BulletError> bulletErrors) {
        String field = fieldExpression.getField();
        Type type = layeredSchema.getType(field);
        if (type == Type.NULL) {
            bulletErrors.add(QueryError.FIELD_NOT_IN_SCHEMA.format(node.getLocation(), field));
        }
        fieldExpression.setType(Type.UNKNOWN);
    }

    static void setType(SubFieldExpressionNode node, FieldExpression subFieldExpression, FieldExpression fieldExpression, List<BulletError> bulletErrors) {
        Optional<List<BulletError>> errors = TypeChecker.validateSubFieldType(node, subFieldExpression, fieldExpression);
        errors.ifPresent(bulletErrors::addAll);
        Type type = fieldExpression.getType();
        if (!Type.isList(type) && !Type.isMap(type)) {
            subFieldExpression.setType(Type.UNKNOWN);
        } else {
            subFieldExpression.setType(type.getSubType());
        }
    }

    static void setType(ListExpressionNode node, ListExpression listExpression, List<BulletError> bulletErrors) {
        Optional<List<BulletError>> errors = TypeChecker.validateListSubTypes(node, listExpression);
        errors.ifPresent(bulletErrors::addAll);
        if (errors.isPresent()) {
            listExpression.setType(Type.UNKNOWN);
        } else {
            setListType(listExpression);
        }
    }

    // First argument is either UnaryExpressionNode or NullPredicateNode
    static void setType(ExpressionNode node, UnaryExpression unaryExpression, List<BulletError> bulletErrors) {
        Optional<List<BulletError>> errors = TypeChecker.validateUnaryType(node, unaryExpression);
        errors.ifPresent(bulletErrors::addAll);
        setUnaryType(unaryExpression);
    }

    static void setType(NAryExpressionNode node, NAryExpression nAryExpression, List<BulletError> bulletErrors) {
        Optional<List<BulletError>> errors = TypeChecker.validateNAryType(node, nAryExpression);
        errors.ifPresent(bulletErrors::addAll);
        if (errors.isPresent()) {
            nAryExpression.setType(Type.UNKNOWN);
        } else {
            setNAryType(nAryExpression);
        }
    }

    static void setType(GroupOperationNode node, Expression expression, Expression operand, List<BulletError> bulletErrors) {
        GroupOperation.GroupOperationType op = node.getOp();
        if (op == GroupOperation.GroupOperationType.COUNT) {
            expression.setType(Type.LONG);
            return;
        }
        Optional<List<BulletError>> errors = TypeChecker.validateNumericType(node, operand);
        errors.ifPresent(bulletErrors::addAll);
        if (errors.isPresent()) {
            expression.setType(Type.DOUBLE);
        } else {
            setAggregateType(expression, op, operand);
        }
    }

    static void setType(CountDistinctNode node, Expression expression, List<Expression> expressions, List<BulletError> bulletErrors) {
        Optional<List<BulletError>> errors = TypeChecker.validatePrimitiveTypes(node, expressions);
        errors.ifPresent(bulletErrors::addAll);
        expression.setType(Type.LONG);
    }

    static void setType(CastExpressionNode node, CastExpression castExpression, List<BulletError> bulletErrors) {
        Optional<List<BulletError>> errors = TypeChecker.validateCastType(node, castExpression);
        errors.ifPresent(bulletErrors::addAll);
        castExpression.setType(castExpression.getCastType());
    }

    static void setType(BinaryExpressionNode node, BinaryExpression binaryExpression, List<BulletError> bulletErrors) {
        Optional<List<BulletError>> errors = TypeChecker.validateBinaryType(node, binaryExpression);
        errors.ifPresent(bulletErrors::addAll);
        setBinaryType(binaryExpression, errors.isPresent());
    }

    private static void setListType(ListExpression listExpression) {
        // Assume non-empty list
        Type subType = listExpression.getValues().get(0).getType();
        Type listType = Type.LISTS.stream().filter(type -> type.getSubType().equals(subType)).findFirst().orElse(Type.UNKNOWN);
        listExpression.setType(listType);
    }

    static void setUnaryType(UnaryExpression unaryExpression) {
        switch (unaryExpression.getOp()) {
            case NOT:
            case IS_NULL:
            case IS_NOT_NULL:
                unaryExpression.setType(Type.BOOLEAN);
                break;
            case SIZE_OF:
                unaryExpression.setType(Type.INTEGER);
                break;
            default:
                // Unreachable normally
                throw new IllegalArgumentException("This is not a supported unary operation: " + unaryExpression.getOp());
        }
    }

    static void setNAryType(NAryExpression nAryExpression) {
        // only IF is supported at the moment
        if (nAryExpression.getOp() == Operation.IF) {
            nAryExpression.setType(nAryExpression.getOperands().get(1).getType());
            return;
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported n-ary operation: " + nAryExpression.getOp());
    }

    static void setAggregateType(Expression expression, GroupOperation.GroupOperationType op, Expression operand) {
        switch (op) {
            case SUM:
            case MIN:
            case MAX:
                expression.setType(operand.getType());
                break;
            case AVG:
                expression.setType(Type.DOUBLE);
                break;
            default:
                // Unreachable normally
                throw new IllegalArgumentException("This is not a supported group operation: " + op);
        }
    }

    static void setBinaryType(BinaryExpression binaryExpression, boolean hasErrors) {
        Type leftType = binaryExpression.getLeft().getType();
        Type rightType = binaryExpression.getRight().getType();
        switch (binaryExpression.getOp()) {
            case ADD:
            case SUB:
            case MUL:
            case DIV:
                if (hasErrors) {
                    binaryExpression.setType(Type.DOUBLE);
                } else if (leftType == Type.DOUBLE || rightType == Type.DOUBLE) {
                    binaryExpression.setType(Type.DOUBLE);
                } else if (leftType == Type.FLOAT || rightType == Type.FLOAT) {
                    binaryExpression.setType(Type.FLOAT);
                } else if (leftType == Type.LONG || rightType == Type.LONG) {
                    binaryExpression.setType(Type.LONG);
                } else {
                    binaryExpression.setType(Type.INTEGER);
                }
                break;
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
            case NOT_REGEX_LIKE:
            case NOT_REGEX_LIKE_ANY:
            case SIZE_IS:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
            case IN:
            case NOT_IN:
            case AND:
            case OR:
            case XOR:
                binaryExpression.setType(Type.BOOLEAN);
                break;
            case FILTER:
                if (hasErrors) {
                    binaryExpression.setType(Type.UNKNOWN);
                } else {
                    binaryExpression.setType(leftType);
                }
                break;
            default:
                // Unreachable normally
                throw new IllegalArgumentException("This is not a supported binary operation: " + binaryExpression.getOp());
        }
    }
}
