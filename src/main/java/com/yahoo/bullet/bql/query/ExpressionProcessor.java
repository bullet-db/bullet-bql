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
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.parsing.expressions.BinaryExpression;
import com.yahoo.bullet.parsing.expressions.CastExpression;
import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.parsing.expressions.ListExpression;
import com.yahoo.bullet.parsing.expressions.NAryExpression;
import com.yahoo.bullet.parsing.expressions.Operation;
import com.yahoo.bullet.parsing.expressions.UnaryExpression;
import com.yahoo.bullet.parsing.expressions.ValueExpression;

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
        FieldExpression expression = new FieldExpression(node.getField().getValue(),
                                                         node.getIndex(),
                                                         node.getKey() != null ? node.getKey().getValue() : null,
                                                         node.getSubKey() != null ? node.getSubKey().getValue() : null,
                                                         node.getType());
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitListExpression(ListExpressionNode node, Map<ExpressionNode, Expression> context) {
        ListExpression expression = new ListExpression(node.getExpressions().stream().map(processFunc(context)).collect(Collectors.toList()));
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitNullPredicate(NullPredicateNode node, Map<ExpressionNode, Expression> context) {
        Operation op = node.isNot() ? Operation.IS_NOT_NULL : Operation.IS_NULL;
        UnaryExpression expression = new UnaryExpression(process(node.getExpression(), context), op);
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitUnaryExpression(UnaryExpressionNode node, Map<ExpressionNode, Expression> context) {
        UnaryExpression expression = new UnaryExpression(process(node.getExpression(), context), node.getOp());
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitNAryExpression(NAryExpressionNode node, Map<ExpressionNode, Expression> context) {
        List<Expression> operands = node.getExpressions().stream().map(processFunc(context)).collect(Collectors.toList());
        NAryExpression expression = new NAryExpression(operands, node.getOp());
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitGroupOperation(GroupOperationNode node, Map<ExpressionNode, Expression> context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitCountDistinct(CountDistinctNode node, Map<ExpressionNode, Expression> context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitDistribution(DistributionNode node, Map<ExpressionNode, Expression> context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitTopK(TopKNode node, Map<ExpressionNode, Expression> context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Expression visitCastExpression(CastExpressionNode node, Map<ExpressionNode, Expression> context) {
        CastExpression expression = new CastExpression(process(node.getExpression(), context), node.getCastType());
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitBinaryExpression(BinaryExpressionNode node, Map<ExpressionNode, Expression> context) {
        BinaryExpression expression = new BinaryExpression(process(node.getLeft(), context),
                                                           process(node.getRight(), context),
                                                           node.getOp(),
                                                           node.getModifier());
        context.put(node, expression);
        return expression;
    }

    @Override
    protected Expression visitParenthesesExpression(ParenthesesExpressionNode node, Map<ExpressionNode, Expression> context) {
        Expression expression = process(node.getExpression(), context);
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
