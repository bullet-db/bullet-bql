/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/AstVisitor.java
 */
package com.yahoo.bullet.bql.tree;

import javax.annotation.Nullable;

public abstract class ASTVisitor<R, C> {
    /**
     * Process a {@link Node}, which will trigger visitor pattern.
     *
     * @param node A {@link Node}.
     * @return A {@link R}.
     */
    public R process(Node node) {
        return process(node, null);
    }

    /**
     * Process a {@link Node} with context, which will trigger visitor pattern.
     *
     * @param node    A {@link Node}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    public R process(Node node, @Nullable C context) {
        return node != null ? node.accept(this, context) : null;
    }

    /**
     * Visit a {@link Node} with passed in context.
     *
     * @param node    A {@link Node}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitNode(Node node, C context) {
        return null;
    }

    /**
     * Visit a {@link QueryNode} with passed in context.
     *
     * @param node    A {@link QueryNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitQuery(QueryNode node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link SelectNode} with passed in context.
     *
     * @param node    A {@link SelectNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitSelect(SelectNode node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link SelectItemNode} with passed in context.
     *
     * @param node    A {@link SelectItemNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitSelectItem(SelectItemNode node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link StreamNode} with passed in context.
     *
     * @param node    A {@link StreamNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitStream(StreamNode node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link GroupByNode} with passed in context.
     *
     * @param node    A {@link GroupByNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitGroupBy(GroupByNode node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit an {@link OrderByNode} with passed in context.
     *
     * @param node    An {@link OrderByNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitOrderBy(OrderByNode node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link SortItemNode} with passed in context.
     *
     * @param node    A {@link SortItemNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitSortItem(SortItemNode node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link WindowNode} with passed in context.
     *
     * @param node    A {@link WindowNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitWindow(WindowNode node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link WindowIncludeNode} with passed in context.
     *
     * @param node    A {@link WindowIncludeNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitWindowInclude(WindowIncludeNode node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit an {@link ExpressionNode} with passed in context.
     *
     * @param node    An {@link ExpressionNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitExpression(ExpressionNode node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link ListExpressionNode} with passed in context.
     *
     * @param node    A {@link ListExpressionNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitListExpression(ListExpressionNode node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link NullPredicateNode} with passed in context.
     *
     * @param node    A {@link NullPredicateNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitNullPredicate(NullPredicateNode node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link UnaryExpressionNode} with passed in context.
     *
     * @param node    A {@link UnaryExpressionNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitUnaryExpression(UnaryExpressionNode node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link NAryExpressionNode} with passed in context.
     *
     * @param node    A {@link NAryExpressionNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitNAryExpression(NAryExpressionNode node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link GroupOperationNode} with passed in context.
     *
     * @param node    A {@link GroupOperationNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitGroupOperation(GroupOperationNode node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link CountDistinctNode} with passed in context.
     *
     * @param node    A {@link CountDistinctNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitCountDistinct(CountDistinctNode node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link DistributionNode} with passed in context.
     *
     * @param node    A {@link DistributionNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitDistribution(DistributionNode node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link TopKNode} with passed in context.
     *
     * @param node    A {@link TopKNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitTopK(TopKNode node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link CastExpressionNode} with passed in context.
     *
     * @param node    A {@link CastExpressionNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitCastExpression(CastExpressionNode node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link BinaryExpressionNode} with passed in context.
     *
     * @param node    A {@link BinaryExpressionNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitBinaryExpression(BinaryExpressionNode node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link ParenthesesExpressionNode} with passed in context.
     *
     * @param node    A {@link ParenthesesExpressionNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitParenthesesExpression(ParenthesesExpressionNode node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit an {@link IdentifierNode} with passed in context.
     *
     * @param node    An {@link IdentifierNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitIdentifier(IdentifierNode node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link LiteralNode} with passed in context.
     *
     * @param node    A {@link LiteralNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitLiteral(LiteralNode node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link NullLiteralNode} with passed in context.
     *
     * @param node    A {@link NullLiteralNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitNullLiteral(NullLiteralNode node, C context) {
        return visitLiteral(node, context);
    }

    /**
     * Visit a {@link StringLiteralNode} with passed in context.
     *
     * @param node    A {@link StringLiteralNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitStringLiteral(StringLiteralNode node, C context) {
        return visitLiteral(node, context);
    }

    /**
     * Visit a {@link LongLiteralNode} with passed in context.
     *
     * @param node    A {@link LongLiteralNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitLongLiteral(LongLiteralNode node, C context) {
        return visitLiteral(node, context);
    }

    /**
     * Visit a {@link DoubleLiteralNode} with passed in context.
     *
     * @param node    A {@link DoubleLiteralNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitDoubleLiteral(DoubleLiteralNode node, C context) {
        return visitLiteral(node, context);
    }

    /**
     * Visit a {@link DecimalLiteralNode} with passed in context.
     *
     * @param node    A {@link DecimalLiteralNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitDecimalLiteral(DecimalLiteralNode node, C context) {
        return visitLiteral(node, context);
    }

    /**
     * Visit a {@link BooleanLiteralNode} with passed in context.
     *
     * @param node    A {@link BooleanLiteralNode}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitBooleanLiteral(BooleanLiteralNode node, C context) {
        return visitLiteral(node, context);
    }
}
