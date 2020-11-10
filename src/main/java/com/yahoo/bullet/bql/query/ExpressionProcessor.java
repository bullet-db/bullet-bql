/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.temp.QuerySchema;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NullPredicateNode;
import com.yahoo.bullet.bql.tree.ParenthesesExpressionNode;
import com.yahoo.bullet.bql.tree.SubFieldExpressionNode;
import com.yahoo.bullet.bql.tree.SubSubFieldExpressionNode;
import com.yahoo.bullet.bql.tree.TopKNode;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ExpressionProcessor extends DefaultTraversalVisitor<Expression, QuerySchema> {
    private static final ExpressionProcessor INSTANCE = new ExpressionProcessor();

    @Override
    public Expression process(Node node) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    public Expression process(Node node, QuerySchema schema) {
        Expression expression = schema.get((ExpressionNode) node);
        if (expression != null) {
            return expression;
        }
        return super.process(node, schema);
    }

    @Override
    protected Expression visitNode(Node node, QuerySchema schema) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitExpression(ExpressionNode node, QuerySchema schema) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitFieldExpression(FieldExpressionNode node, QuerySchema schema) {
        FieldExpression expression = new FieldExpression(node.getField().getValue());
        expression.setType(node.getType());
        schema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitSubFieldExpression(SubFieldExpressionNode node, QuerySchema schema) {
        FieldExpression fieldExpression = (FieldExpression) process(node.getField(), schema);
        FieldExpression subFieldExpression;
        if (node.getIndex() != null) {
            subFieldExpression = new FieldExpression(fieldExpression.getField(), node.getIndex());
        } else {
            subFieldExpression = new FieldExpression(fieldExpression.getField(), node.getKey().getValue());
        }
        subFieldExpression.setType(node.getType());
        schema.put(node, subFieldExpression);
        return subFieldExpression;
    }

    @Override
    protected Expression visitSubSubFieldExpression(SubSubFieldExpressionNode node, QuerySchema schema) {
        FieldExpression subFieldExpression = (FieldExpression) process(node.getSubField(), schema);
        FieldExpression subSubFieldExpression;
        if (subFieldExpression.getIndex() != null) {
            subSubFieldExpression = new FieldExpression(subFieldExpression.getField(), subFieldExpression.getIndex(), node.getSubKey().getValue());
        } else if (subFieldExpression.getKey() != null) {
            subSubFieldExpression = new FieldExpression(subFieldExpression.getField(), subFieldExpression.getKey(), node.getSubKey().getValue());
        } else {
            // Special case where the subFieldExpression is replaced with a FieldExpression
            subSubFieldExpression = new FieldExpression(subFieldExpression.getField(), node.getSubKey().getValue());
        }
        subSubFieldExpression.setType(node.getType());
        schema.put(node, subSubFieldExpression);
        return subSubFieldExpression;
    }

    /*
    If a subfield's base field has an alias, a mapping must exist for just the simple field alone.
    This method checks if that mapping exists and returns the alias if it does.
    */
    /*
    private String getMappedField(FieldExpressionNode node, QuerySchema schema) {
        String field = node.getField().getValue();
        if (!node.hasIndexOrKey()) {
            return field;
        }
        // TODO optimize
        FieldExpression expression = (FieldExpression) schema.get(new FieldExpressionNode(new IdentifierNode(field, false, null), null, null, null, null, null));
        if (expression != null) {
            return expression.getField();
        }
        return field;
    }
    */

    @Override
    protected Expression visitListExpression(ListExpressionNode node, QuerySchema schema) {
        List<Expression> expressions = node.getExpressions().stream().map(processFunc(schema)).collect(Collectors.toCollection(ArrayList::new));
        ListExpression listExpression = new ListExpression(expressions);
        schema.put(node, listExpression);
        return listExpression;
    }

    @Override
    protected Expression visitNullPredicate(NullPredicateNode node, QuerySchema schema) {
        Expression operand = process(node.getExpression(), schema);
        Operation op = node.isNot() ? Operation.IS_NOT_NULL : Operation.IS_NULL;
        UnaryExpression expression = new UnaryExpression(operand, op);
        schema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitUnaryExpression(UnaryExpressionNode node, QuerySchema schema) {
        Expression operand = process(node.getExpression(), schema);
        UnaryExpression expression = new UnaryExpression(operand, node.getOp());
        schema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitNAryExpression(NAryExpressionNode node, QuerySchema schema) {
        List<Expression> operands = node.getExpressions().stream().map(processFunc(schema)).collect(Collectors.toCollection(ArrayList::new));
        NAryExpression expression = new NAryExpression(operands, node.getOp());
        schema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitGroupOperation(GroupOperationNode node, QuerySchema schema) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitCountDistinct(CountDistinctNode node, QuerySchema schema) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitDistribution(DistributionNode node, QuerySchema schema) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitTopK(TopKNode node, QuerySchema schema) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitCastExpression(CastExpressionNode node, QuerySchema schema) {
        Expression operand = process(node.getExpression(), schema);
        CastExpression expression = new CastExpression(operand, node.getCastType());
        schema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitBinaryExpression(BinaryExpressionNode node, QuerySchema schema) {
        Expression left = process(node.getLeft(), schema);
        Expression right = process(node.getRight(), schema);
        BinaryExpression expression = new BinaryExpression(left, right, node.getOp());
        schema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitParenthesesExpression(ParenthesesExpressionNode node, QuerySchema schema) {
        Expression expression = process(node.getExpression(), schema);
        schema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitLiteral(LiteralNode node, QuerySchema schema) {
        ValueExpression expression = new ValueExpression(node.getValue());
        schema.put(node, expression);
        return expression;
    }

    public static Expression visit(Node node, QuerySchema schema) {
        return INSTANCE.process(node, schema);
    }

    public static void visit(Collection<? extends Node> nodes, QuerySchema schema) {
        nodes.forEach(INSTANCE.process(schema));
    }
}
