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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Checks whether or not a given expression is computable given a set of existing fields.
 */
public class ComputableProcessor extends DefaultTraversalVisitor<Boolean, ProcessedQuery> {
    private static final ComputableProcessor INSTANCE = new ComputableProcessor();

    @Override
    public Boolean process(Node node) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Boolean visitExpression(ExpressionNode node, ProcessedQuery context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Boolean visitFieldExpression(FieldExpressionNode node, ProcessedQuery context) {
        return context.getSelectNames().contains(node.getField().getValue());
    }

    @Override
    protected Boolean visitListExpression(ListExpressionNode node, ProcessedQuery context) {
        return node.getExpressions().stream().allMatch(expressionNode -> process(expressionNode, context));
    }

    @Override
    protected Boolean visitNullPredicate(NullPredicateNode node, ProcessedQuery context) {
        return process(node.getExpression(), context);
    }

    @Override
    protected Boolean visitUnaryExpression(UnaryExpressionNode node, ProcessedQuery context) {
        return process(node.getExpression(), context);
    }

    @Override
    protected Boolean visitNAryExpression(NAryExpressionNode node, ProcessedQuery context) {
        return node.getExpressions().stream().allMatch(expressionNode -> process(expressionNode, context));
    }

    @Override
    protected Boolean visitGroupOperation(GroupOperationNode node, ProcessedQuery context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Boolean visitCountDistinct(CountDistinctNode node, ProcessedQuery context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Boolean visitDistribution(DistributionNode node, ProcessedQuery context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Boolean visitTopK(TopKNode node, ProcessedQuery context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected Boolean visitCastExpression(CastExpressionNode node, ProcessedQuery context) {
        return process(node.getExpression(), context);
    }

    @Override
    protected Boolean visitBinaryExpression(BinaryExpressionNode node, ProcessedQuery context) {
        return process(node.getLeft(), context) && process(node.getRight(), context);
    }

    @Override
    protected Boolean visitParenthesesExpression(ParenthesesExpressionNode node, ProcessedQuery context) {
        return process(node.getExpression(), context);
    }

    @Override
    protected Boolean visitLiteral(LiteralNode node, ProcessedQuery context) {
        return true;
    }

    /**
     * Returns the expressions that are not computable.
     */
    public static List<ExpressionNode> visit(Collection<ExpressionNode> nodes, ProcessedQuery processedQuery) {
        return nodes.stream().filter(node -> !INSTANCE.process(node, processedQuery)).collect(Collectors.toList());
    }
}
