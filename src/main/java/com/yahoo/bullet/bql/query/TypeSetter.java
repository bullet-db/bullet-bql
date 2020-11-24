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
    public static void setType(FieldExpressionNode node, FieldExpression fieldExpression, QuerySchema querySchema) {
        if (node.getType() != null) {
            fieldExpression.setType(node.getType());
            return;
        }
        Type type = querySchema.getType(fieldExpression.getField());
        if (type == null) {
            querySchema.addTypeError(node, "The field " + fieldExpression.getField() + " does not exist in the schema.");
        }
        fieldExpression.setType(Type.UNKNOWN);
    }

    public static void setType(SubFieldExpressionNode node, FieldExpression subFieldExpression, FieldExpression fieldExpression, QuerySchema querySchema) {
        if (node.getType() != null) {
            subFieldExpression.setType(node.getType());
            return;
        }
        Optional<List<BulletError>> errors = TypeChecker.validateSubFieldType(node, fieldExpression);
        if (errors.isPresent()) {
            querySchema.addErrors(errors.get());
            subFieldExpression.setType(Type.UNKNOWN);
        } else {
            subFieldExpression.setType(fieldExpression.getType().getSubType());
        }
    }

    public static void setType(ListExpressionNode node, ListExpression listExpression, QuerySchema querySchema) {
        Optional<List<BulletError>> errors = TypeChecker.validateListSubTypes(node, listExpression);
        if (errors.isPresent()) {
            querySchema.addErrors(errors.get());
            listExpression.setType(Type.UNKNOWN);
        } else {
            setListType(listExpression);
        }
    }

    // First argument is either UnaryExpressionNode or NullPredicateNode
    public static void setType(ExpressionNode node, UnaryExpression unaryExpression, QuerySchema querySchema) {
        Optional<List<BulletError>> errors = TypeChecker.validateUnaryType(node, unaryExpression);
        errors.ifPresent(querySchema::addErrors);
        setUnaryType(unaryExpression);
    }

    public static void setType(NAryExpressionNode node, NAryExpression nAryExpression, QuerySchema querySchema) {
        Optional<List<BulletError>> errors = TypeChecker.validateNAryType(node, nAryExpression);
        if (errors.isPresent()) {
            querySchema.addErrors(errors.get());
            nAryExpression.setType(Type.UNKNOWN);
        } else {
            setNAryType(nAryExpression);
        }
    }

    public static void setType(GroupOperationNode node, Expression expression, Expression operand, QuerySchema querySchema) {
        GroupOperation.GroupOperationType op = node.getOp();
        if (op == GroupOperation.GroupOperationType.COUNT) {
            expression.setType(Type.LONG);
            return;
        }
        Optional<List<BulletError>> errors = TypeChecker.validateNumericType(node, operand);
        if (errors.isPresent()) {
            querySchema.addErrors(errors.get());
            expression.setType(Type.DOUBLE);
        } else {
            setAggregateType(expression, op, operand);
        }
    }

    public static void setType(CountDistinctNode node, Expression expression, List<Expression> expressions, QuerySchema querySchema) {
        Optional<List<BulletError>> errors = TypeChecker.validatePrimitiveTypes(node, expressions);
        errors.ifPresent(querySchema::addErrors);
        expression.setType(Type.LONG);
    }


    public static void setType(CastExpressionNode node, CastExpression castExpression, QuerySchema querySchema) {
        Optional<List<BulletError>> errors = TypeChecker.validateCastType(node, castExpression);
        errors.ifPresent(querySchema::addErrors);
        castExpression.setType(castExpression.getCastType());
    }

    public static void setType(BinaryExpressionNode node, BinaryExpression binaryExpression, QuerySchema querySchema) {
        Optional<List<BulletError>> errors = TypeChecker.validateBinaryType(node, binaryExpression);
        errors.ifPresent(querySchema::addErrors);
        setBinaryType(binaryExpression, errors.isPresent());
    }

    private static void setListType(ListExpression listExpression) {
        // Assume non-empty list
        Type subType = listExpression.getValues().get(0).getType();
        Type listType = Type.LISTS.stream().filter(type -> type.getSubType().equals(subType)).findFirst().orElse(Type.UNKNOWN);
        listExpression.setType(listType);
    }

    private static void setUnaryType(UnaryExpression unaryExpression) {
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

    private static void setNAryType(NAryExpression nAryExpression) {
        // only IF is supported at the moment
        if (nAryExpression.getOp() == Operation.IF) {
            nAryExpression.setType(nAryExpression.getOperands().get(1).getType());
            return;
        }
        // Unreachable normally
        throw new IllegalArgumentException("This is not a supported n-ary operation: " + nAryExpression.getOp());
    }

    private static void setAggregateType(Expression expression, GroupOperation.GroupOperationType op, Expression operand) {
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

    private static void setBinaryType(BinaryExpression binaryExpression, boolean hasErrors) {
        Type leftType = binaryExpression.getLeft().getType();
        Type rightType = binaryExpression.getRight().getType();
        switch (binaryExpression.getOp()) {
            case ADD:
            case SUB:
            case MUL:
            case DIV:
                if (hasErrors || leftType == Type.DOUBLE || rightType == Type.DOUBLE) {
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
                binaryExpression.setType(hasErrors ? Type.UNKNOWN : leftType);
                break;
            default:
                // Unreachable normally
                throw new IllegalArgumentException("This is not a supported binary operation: " + binaryExpression.getOp());
        }
    }
}
