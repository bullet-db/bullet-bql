package com.yahoo.bullet.bql.query;

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

import static com.yahoo.bullet.bql.query.TypeSetter.setType;

public class ExpressionVisitor extends DefaultTraversalVisitor<Expression, QuerySchema> {
    private static final ExpressionVisitor INSTANCE = new ExpressionVisitor();

    @Override
    public Expression process(Node node) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    public Expression process(Node node, QuerySchema querySchema) {
        Expression expression = querySchema.get((ExpressionNode) node);
        if (expression != null) {
            return expression;
        }
        return super.process(node, querySchema);
    }

    @Override
    protected Expression visitNode(Node node, QuerySchema querySchema) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitExpression(ExpressionNode node, QuerySchema querySchema) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitFieldExpression(FieldExpressionNode node, QuerySchema querySchema) {
        FieldExpression expression = new FieldExpression(node.getField().getValue());
        setType(node, expression, querySchema);
        querySchema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitSubFieldExpression(SubFieldExpressionNode node, QuerySchema querySchema) {
        FieldExpression fieldExpression = (FieldExpression) process(node.getField(), querySchema);
        FieldExpression expression;
        if (fieldExpression.getIndex() != null) {
            expression = new FieldExpression(fieldExpression.getField(), fieldExpression.getIndex(), node.getKey().getValue());
        } else if (fieldExpression.getKey() != null) {
            expression = new FieldExpression(fieldExpression.getField(), fieldExpression.getKey(), node.getKey().getValue());
        } else if (node.getIndex() != null) {
            expression = new FieldExpression(fieldExpression.getField(), node.getIndex());
        } else {
            expression = new FieldExpression(fieldExpression.getField(), node.getKey().getValue());
        }
        // TODO
        setType(node, expression, fieldExpression, querySchema);
        querySchema.put(node, expression);
        return expression;
    }
/*
    @Override
    protected Expression visitSubSubFieldExpression(SubSubFieldExpressionNode node, QuerySchema querySchema) {
        FieldExpression subFieldExpression = (FieldExpression) process(node.getSubField(), querySchema);
        FieldExpression expression;
        if (subFieldExpression.getIndex() != null) {
            expression = new FieldExpression(subFieldExpression.getField(), subFieldExpression.getIndex(), node.getSubKey().getValue());
        } else if (subFieldExpression.getKey() != null) {
            expression = new FieldExpression(subFieldExpression.getField(), subFieldExpression.getKey(), node.getSubKey().getValue());
        } else {
            // Special case where the subFieldExpression is replaced with a FieldExpression
            expression = new FieldExpression(subFieldExpression.getField(), node.getSubKey().getValue());
        }
        if (node.getType() != null) {
            expression.setType(node.getType());
        } else {
            setType(node, );
        }
        querySchema.put(node, expression);
        return expression;
    }
*/
    @Override
    protected Expression visitListExpression(ListExpressionNode node, QuerySchema querySchema) {
        List<Expression> expressions = node.getExpressions().stream().map(processFunc(querySchema)).collect(Collectors.toCollection(ArrayList::new));
        ListExpression listExpression = new ListExpression(expressions);
        setType(node, listExpression, querySchema);
        querySchema.put(node, listExpression);
        return listExpression;
    }

    @Override
    protected Expression visitNullPredicate(NullPredicateNode node, QuerySchema querySchema) {
        Expression operand = process(node.getExpression(), querySchema);
        Operation op = node.isNot() ? Operation.IS_NOT_NULL : Operation.IS_NULL;
        UnaryExpression expression = new UnaryExpression(operand, op);
        setType(node, expression, querySchema);
        querySchema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitUnaryExpression(UnaryExpressionNode node, QuerySchema querySchema) {
        Expression operand = process(node.getExpression(), querySchema);
        UnaryExpression expression = new UnaryExpression(operand, node.getOp());
        setType(node, expression, querySchema);
        querySchema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitNAryExpression(NAryExpressionNode node, QuerySchema querySchema) {
        List<Expression> operands = node.getExpressions().stream().map(processFunc(querySchema)).collect(Collectors.toCollection(ArrayList::new));
        NAryExpression expression = new NAryExpression(operands, node.getOp());
        setType(node, expression, querySchema);
        querySchema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitGroupOperation(GroupOperationNode node, QuerySchema querySchema) {
        Expression operand = process(node.getExpression(), querySchema);
        FieldExpression expression = new FieldExpression(node.getName());
        setType(node, expression, operand, querySchema);
        querySchema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitCountDistinct(CountDistinctNode node, QuerySchema querySchema) {
        List<Expression> expressions = node.getExpressions().stream().map(processFunc(querySchema)).collect(Collectors.toList());
        FieldExpression expression = new FieldExpression(node.getName());
        setType(node, expression, expressions, querySchema);
        querySchema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitDistribution(DistributionNode node, QuerySchema querySchema) {
        Expression expression = process(node.getExpression(), querySchema);
        TypeChecker.validateNumericType(node, expression).ifPresent(querySchema::addErrors);
        return null;
    }

    @Override
    protected Expression visitTopK(TopKNode node, QuerySchema querySchema) {
        List<Expression> expressions = node.getExpressions().stream().map(processFunc(querySchema)).collect(Collectors.toList());
        TypeChecker.validatePrimitiveTypes(node, expressions).ifPresent(querySchema::addErrors);
        return null;
    }

    @Override
    protected Expression visitCastExpression(CastExpressionNode node, QuerySchema querySchema) {
        Expression operand = process(node.getExpression(), querySchema);
        CastExpression expression = new CastExpression(operand, node.getCastType());
        setType(node, expression, querySchema);
        querySchema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitBinaryExpression(BinaryExpressionNode node, QuerySchema querySchema) {
        Expression left = process(node.getLeft(), querySchema);
        Expression right = process(node.getRight(), querySchema);
        BinaryExpression expression = new BinaryExpression(left, right, node.getOp());
        setType(node, expression, querySchema);
        querySchema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitParenthesesExpression(ParenthesesExpressionNode node, QuerySchema querySchema) {
        Expression expression = process(node.getExpression(), querySchema);
        querySchema.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitLiteral(LiteralNode node, QuerySchema querySchema) {
        ValueExpression expression = new ValueExpression(node.getValue());
        querySchema.put(node, expression);
        return expression;
    }

    public static Expression visit(Node node, QuerySchema querySchema) {
        return INSTANCE.process(node, querySchema);
    }

    public static void visit(Collection<? extends Node> nodes, QuerySchema querySchema) {
        nodes.forEach(INSTANCE.process(querySchema));
    }
}
