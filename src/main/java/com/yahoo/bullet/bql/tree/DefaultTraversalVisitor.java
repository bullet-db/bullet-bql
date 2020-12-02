/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/DefaultTraversalVisitor.java
 */
package com.yahoo.bullet.bql.tree;

import java.util.function.Consumer;
import java.util.function.Function;

public abstract class DefaultTraversalVisitor<R, C> extends ASTVisitor<R, C> {
    @Override
    protected R visitQuery(QueryNode node, C context) {
        process(node.getSelect(), context);
        process(node.getStream(), context);
        process(node.getWhere(), context);
        process(node.getGroupBy(), context);
        process(node.getHaving(), context);
        process(node.getOrderBy(), context);
        process(node.getWindow(), context);
        return null;
    }

    @Override
    protected R visitSelect(SelectNode node, C context) {
        node.getSelectItems().forEach(process(context));
        return null;
    }

    @Override
    protected R visitSelectItem(SelectItemNode node, C context) {
        process(node.getExpression(), context);
        return null;
    }

    @Override
    protected R visitGroupBy(GroupByNode node, C context) {
        node.getExpressions().forEach(process(context));
        return null;
    }

    @Override
    protected R visitOrderBy(OrderByNode node, C context) {
        node.getSortItems().forEach(process(context));
        return null;
    }

    @Override
    protected R visitSortItem(SortItemNode node, C context) {
        process(node.getExpression(), context);
        return null;
    }

    @Override
    protected R visitWindow(WindowNode node, C context) {
        process(node.getWindowInclude(), context);
        return null;
    }

    @Override
    protected R visitSubFieldExpression(SubFieldExpressionNode node, C context) {
        process(node.getField(), context);
        return null;
    }

    @Override
    protected R visitListExpression(ListExpressionNode node, C context) {
        node.getExpressions().forEach(process(context));
        return null;
    }

    @Override
    protected R visitNullPredicate(NullPredicateNode node, C context) {
        process(node.getExpression(), context);
        return null;
    }

    @Override
    protected R visitUnaryExpression(UnaryExpressionNode node, C context) {
        process(node.getExpression(), context);
        return null;
    }

    @Override
    protected R visitNAryExpression(NAryExpressionNode node, C context) {
        node.getExpressions().forEach(process(context));
        return null;
    }

    @Override
    protected R visitGroupOperation(GroupOperationNode node, C context) {
        process(node.getExpression(), context);
        return null;
    }

    @Override
    protected R visitCountDistinct(CountDistinctNode node, C context) {
        node.getExpressions().forEach(process(context));
        return null;
    }

    @Override
    protected R visitDistribution(DistributionNode node, C context) {
        process(node.getExpression(), context);
        return null;
    }

    @Override
    protected R visitTopK(TopKNode node, C context) {
        node.getExpressions().forEach(process(context));
        return null;
    }

    @Override
    protected R visitCastExpression(CastExpressionNode node, C context) {
        process(node.getExpression(), context);
        return null;
    }

    @Override
    protected R visitBinaryExpression(BinaryExpressionNode node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);
        return null;
    }

    @Override
    protected R visitParenthesesExpression(ParenthesesExpressionNode node, C context) {
        process(node.getExpression(), context);
        return null;
    }

    /**
     * Helper to process nodes with a given context.
     *
     * @param context Context with which to process {@link Node}.
     * @return A consumer for processing {@link Node}.
     */
    protected Consumer<Node> process(C context) {
        return node -> process(node, context);
    }

    /**
     * Helper to process nodes with a given context.
     *
     * @param context Context with which to process {@link Node}.
     * @return A function for processing {@link Node}.
     */
    protected Function<Node, R> processFunc(C context) {
        return node -> process(node, context);
    }
}
