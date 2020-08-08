/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.IdentifierNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NullPredicateNode;
import com.yahoo.bullet.bql.tree.ParenthesesExpressionNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.CastExpression;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.ListExpression;
import com.yahoo.bullet.query.expressions.NAryExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.UnaryExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExpressionProcessor extends DefaultTraversalVisitor<Expression, Map<ExpressionNode, Expression>> {
    private static final ExpressionProcessor INSTANCE = new ExpressionProcessor();

    @Override
    public Expression process(Node node) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    public Expression process(Node node, Map<ExpressionNode, Expression> context) {
        Expression expression = context.get(node);
        if (expression != null) {
            return expression;
        }
        return super.process(node, context);
    }

    @Override
    protected Expression visitNode(Node node, Map<ExpressionNode, Expression> context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitExpression(ExpressionNode node, Map<ExpressionNode, Expression> context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitFieldExpression(FieldExpressionNode node, Map<ExpressionNode, Expression> context) {
        String field = getMappedField(node, context);
        FieldExpression expression;
        if (node.getIndex() != null) {
            if (node.getSubKey() != null) {
                expression = new FieldExpression(field, node.getIndex(), node.getSubKey().getValue());
            } else {
                expression = new FieldExpression(field, node.getIndex());
            }
        } else if (node.getKey() != null) {
            if (node.getSubKey() != null) {
                expression = new FieldExpression(field, node.getKey().getValue(), node.getSubKey().getValue());
            } else {
                expression = new FieldExpression(field, node.getKey().getValue());
            }
        } else {
            expression = new FieldExpression(field);
        }
        expression.setType(node.getType());
        context.put(node, expression);
        return expression;
    }

    private String getMappedField(FieldExpressionNode node, Map<ExpressionNode, Expression> context) {
        String field = node.getField().getValue();
        if (!node.hasIndexOrKey()) {
            return field;
        }
        FieldExpression expression = (FieldExpression) context.get(new FieldExpressionNode(new IdentifierNode(field, false, null), null, null, null, null, null));
        if (expression != null) {
            return expression.getField();
        }
        return field;
    }

    @Override
    protected Expression visitListExpression(ListExpressionNode node, Map<ExpressionNode, Expression> context) {
        List<Expression> expressions = node.getExpressions().stream().map(processFunc(context)).collect(Collectors.toList());
        if (expressions.contains(null)) {
            return null;
        }
        ListExpression listExpression = new ListExpression(expressions);
        context.put(node, listExpression);
        return listExpression;
    }

    @Override
    protected Expression visitNullPredicate(NullPredicateNode node, Map<ExpressionNode, Expression> context) {
        Expression operand = process(node.getExpression(), context);
        if (operand == null) {
            return null;
        }
        Operation op = node.isNot() ? Operation.IS_NOT_NULL : Operation.IS_NULL;
        UnaryExpression expression = new UnaryExpression(operand, op);
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitUnaryExpression(UnaryExpressionNode node, Map<ExpressionNode, Expression> context) {
        Expression operand = process(node.getExpression(), context);
        if (operand == null) {
            return null;
        }
        UnaryExpression expression = new UnaryExpression(operand, node.getOp());
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitNAryExpression(NAryExpressionNode node, Map<ExpressionNode, Expression> context) {
        List<Expression> operands = node.getExpressions().stream().map(processFunc(context)).collect(Collectors.toList());
        if (operands.contains(null)) {
            return null;
        }
        NAryExpression expression = new NAryExpression(operands, node.getOp());
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitGroupOperation(GroupOperationNode node, Map<ExpressionNode, Expression> context) {
        Expression operand = process(node.getExpression(), context);
        if (operand == null) {
            return null;
        }
        FieldExpression expression = new FieldExpression(node.getName());
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitCountDistinct(CountDistinctNode node, Map<ExpressionNode, Expression> context) {
        List<Expression> expressions = node.getExpressions().stream().map(processFunc(context)).collect(Collectors.toList());
        if (expressions.contains(null)) {
            return null;
        }
        FieldExpression expression = new FieldExpression(node.getName());
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitCastExpression(CastExpressionNode node, Map<ExpressionNode, Expression> context) {
        Expression operand = process(node.getExpression(), context);
        if (operand == null) {
            return null;
        }
        CastExpression expression = new CastExpression(operand, node.getCastType());
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitBinaryExpression(BinaryExpressionNode node, Map<ExpressionNode, Expression> context) {
        Expression left = process(node.getLeft(), context);
        Expression right = process(node.getRight(), context);
        if (left == null || right == null) {
            return null;
        }
        BinaryExpression expression = new BinaryExpression(left, right, node.getOp());
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitParenthesesExpression(ParenthesesExpressionNode node, Map<ExpressionNode, Expression> context) {
        Expression expression = process(node.getExpression(), context);
        if (expression == null) {
            return null;
        }
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitLiteral(LiteralNode node, Map<ExpressionNode, Expression> context) {
        ValueExpression expression = new ValueExpression(node.getValue());
        context.put(node, expression);
        return expression;
    }

    public static Expression visit(Node node, Map<ExpressionNode, Expression> context) {
        return INSTANCE.process(node, context);
    }

    public static void visit(Collection<? extends Node> nodes, Map<ExpressionNode, Expression> context) {
        nodes.forEach(INSTANCE.process(context));
    }
}
