/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.BetweenPredicateNode;
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
import com.yahoo.bullet.bql.tree.TableFunctionNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.query.expressions.CastExpression;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.ListExpression;
import com.yahoo.bullet.query.expressions.NAryExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.UnaryExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.yahoo.bullet.bql.query.TypeSetter.setType;

@RequiredArgsConstructor
public class ExpressionVisitor extends DefaultTraversalVisitor<Expression, LayeredSchema> {
    private final List<BulletError> errors;
    private Map<Node, Expression> mapping = new HashMap<>();

    @Override
    public Expression process(Node node) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    public Expression process(Node node, LayeredSchema layeredSchema) {
        if (node == null) {
            return null;
        }
        Expression expression = mapping.get(node);
        if (expression != null) {
            return expression;
        }
        String name = ((ExpressionNode) node).getName();
        // Type override
        if (node instanceof FieldExpressionNode) {
            Type type = ((FieldExpressionNode) node).getType();
            if (type != null) {
                return field(name, type);
            }
        } else if (node instanceof SubFieldExpressionNode) {
            Type type = ((SubFieldExpressionNode) node).getType();
            if (type != null) {
                return field(name, type);
            }
        }
        Schema.Field field = layeredSchema.getField(name);
        if (field != null) {
            return field(field.getName(), field.getType());
        }
        return super.process(node, layeredSchema);
    }

    private static FieldExpression field(String name, Type type) {
        FieldExpression expression = new FieldExpression(name);
        expression.setType(type);
        return expression;
    }

    @Override
    protected Expression visitNode(Node node, LayeredSchema layeredSchema) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitExpression(ExpressionNode node, LayeredSchema layeredSchema) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitFieldExpression(FieldExpressionNode node, LayeredSchema layeredSchema) {
        FieldExpression expression = new FieldExpression(node.getField().getValue());
        setType(node, expression, layeredSchema, errors);
        mapping.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitSubFieldExpression(SubFieldExpressionNode node, LayeredSchema layeredSchema) {
        FieldExpression fieldExpression = (FieldExpression) process(node.getField(), layeredSchema);
        FieldExpression subFieldExpression;
        if (node.getIndex() != null) {
            subFieldExpression = new FieldExpression(fieldExpression, node.getIndex());
        } else if (node.getKey() != null) {
            subFieldExpression = new FieldExpression(fieldExpression, node.getKey().getValue());
        } else if (node.getExpressionKey() != null) {
            subFieldExpression = new FieldExpression(fieldExpression, process(node.getExpressionKey(), layeredSchema));
        } else {
            subFieldExpression = new FieldExpression(fieldExpression, node.getStringKey());
        }
        setType(node, subFieldExpression, fieldExpression, errors);
        mapping.put(node, subFieldExpression);
        return subFieldExpression;
    }

    @Override
    protected Expression visitListExpression(ListExpressionNode node, LayeredSchema layeredSchema) {
        List<Expression> expressions = node.getExpressions().stream().map(processFunc(layeredSchema)).collect(Collectors.toCollection(ArrayList::new));
        ListExpression listExpression = new ListExpression(expressions);
        setType(node, listExpression, errors);
        mapping.put(node, listExpression);
        return listExpression;
    }

    @Override
    protected Expression visitNullPredicate(NullPredicateNode node, LayeredSchema layeredSchema) {
        Expression operand = process(node.getExpression(), layeredSchema);
        Operation op = node.isNot() ? Operation.IS_NOT_NULL : Operation.IS_NULL;
        UnaryExpression expression = new UnaryExpression(operand, op);
        setType(node, expression, errors);
        mapping.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitBetweenPredicate(BetweenPredicateNode node, LayeredSchema layeredSchema) {
        Expression value = process(node.getExpression(), layeredSchema);
        Expression lower = process(node.getLower(), layeredSchema);
        Expression upper = process(node.getUpper(), layeredSchema);
        NAryExpression expression;
        if (node.isNot()) {
            expression = new NAryExpression(Arrays.asList(value, lower, upper), Operation.NOT_BETWEEN);
        } else {
            expression = new NAryExpression(Arrays.asList(value, lower, upper), Operation.BETWEEN);
        }
        setType(node, expression, value, lower, upper, errors);
        mapping.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitUnaryExpression(UnaryExpressionNode node, LayeredSchema layeredSchema) {
        Expression operand = process(node.getExpression(), layeredSchema);
        UnaryExpression expression = new UnaryExpression(operand, node.getOp());
        setType(node, expression, errors);
        mapping.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitNAryExpression(NAryExpressionNode node, LayeredSchema layeredSchema) {
        List<Expression> operands = node.getExpressions().stream().map(processFunc(layeredSchema)).collect(Collectors.toCollection(ArrayList::new));
        NAryExpression expression = new NAryExpression(operands, node.getOp());
        setType(node, expression, errors);
        mapping.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitGroupOperation(GroupOperationNode node, LayeredSchema layeredSchema) {
        Expression operand = process(node.getExpression(), layeredSchema);
        FieldExpression expression = new FieldExpression(node.getName());
        setType(node, expression, operand, errors);
        mapping.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitCountDistinct(CountDistinctNode node, LayeredSchema layeredSchema) {
        List<Expression> expressions = node.getExpressions().stream().map(processFunc(layeredSchema)).collect(Collectors.toList());
        FieldExpression expression = new FieldExpression(node.getName());
        setType(node, expression, expressions, errors);
        mapping.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitDistribution(DistributionNode node, LayeredSchema layeredSchema) {
        Expression expression = process(node.getExpression(), layeredSchema);
        TypeChecker.validateNumericType(node, expression).ifPresent(errors::addAll);
        return null;
    }

    @Override
    protected Expression visitTopK(TopKNode node, LayeredSchema layeredSchema) {
        List<Expression> expressions = node.getExpressions().stream().map(processFunc(layeredSchema)).collect(Collectors.toList());
        TypeChecker.validatePrimitiveTypes(node, expressions).ifPresent(errors::addAll);
        return null;
    }

    @Override
    protected Expression visitCastExpression(CastExpressionNode node, LayeredSchema layeredSchema) {
        Expression operand = process(node.getExpression(), layeredSchema);
        CastExpression expression = new CastExpression(operand, node.getCastType());
        setType(node, expression, errors);
        mapping.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitTableFunction(TableFunctionNode node, LayeredSchema layeredSchema) {
        Expression expression = process(node.getExpression(), layeredSchema);
        TypeChecker.validateTableFunctionType(node, expression).ifPresent(errors::addAll);
        return null;
    }

    @Override
    protected Expression visitBinaryExpression(BinaryExpressionNode node, LayeredSchema layeredSchema) {
        Expression left = process(node.getLeft(), layeredSchema);
        Expression right = process(node.getRight(), layeredSchema);
        BinaryExpression expression = new BinaryExpression(left, right, node.getOp());
        setType(node, expression, errors);
        mapping.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitParenthesesExpression(ParenthesesExpressionNode node, LayeredSchema layeredSchema) {
        Expression expression = process(node.getExpression(), layeredSchema);
        mapping.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitLiteral(LiteralNode node, LayeredSchema layeredSchema) {
        ValueExpression expression = new ValueExpression(node.getValue());
        mapping.put(node, expression);
        return expression;
    }

    void resetMapping() {
        mapping.clear();
    }
}
