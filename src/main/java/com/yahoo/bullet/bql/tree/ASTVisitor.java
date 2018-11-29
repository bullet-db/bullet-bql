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
        return node.accept(this, context);
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
     * Visit an {@link Expression} with passed in context.
     *
     * @param node    An {@link Expression}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitExpression(Expression node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link BetweenPredicate} with passed in context.
     *
     * @param node    A {@link BetweenPredicate}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitBetweenPredicate(BetweenPredicate node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link ComparisonExpression} with passed in context.
     *
     * @param node    A {@link ComparisonExpression}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitComparisonExpression(ComparisonExpression node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link Literal} with passed in context.
     *
     * @param node    A {@link Literal}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitLiteral(Literal node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link DoubleLiteral} with passed in context.
     *
     * @param node    A {@link DoubleLiteral}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitDoubleLiteral(DoubleLiteral node, C context) {
        return visitLiteral(node, context);
    }

    /**
     * Visit a {@link DecimalLiteral} with passed in context.
     *
     * @param node    A {@link DecimalLiteral}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitDecimalLiteral(DecimalLiteral node, C context) {
        return visitLiteral(node, context);
    }

    /**
     * Visit a {@link Statement} with passed in context.
     *
     * @param node    A {@link Statement}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitStatement(Statement node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link Query} with passed in context.
     *
     * @param node    A {@link Query}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitQuery(Query node, C context) {
        return visitStatement(node, context);
    }

    /**
     * Visit a {@link With} with passed in context.
     *
     * @param node    A {@link With}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitWith(With node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link WithQuery} with passed in context.
     *
     * @param node    A {@link WithQuery}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitWithQuery(WithQuery node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link Select} with passed in context.
     *
     * @param node    A {@link Select}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitSelect(Select node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link Relation} with passed in context.
     *
     * @param node    A {@link Relation}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitRelation(Relation node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link QueryBody} with passed in context.
     *
     * @param node    A {@link QueryBody}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitQueryBody(QueryBody node, C context) {
        return visitRelation(node, context);
    }

    /**
     * Visit an {@link OrderBy} with passed in context.
     *
     * @param node    An {@link OrderBy}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitOrderBy(OrderBy node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link QuerySpecification} with passed in context.
     *
     * @param node    A {@link QuerySpecification}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitQuerySpecification(QuerySpecification node, C context) {
        return visitQueryBody(node, context);
    }

    /**
     * Visit a {@link InPredicate} with passed in context.
     *
     * @param node    A {@link InPredicate}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitInPredicate(InPredicate node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link ContainsPredicate} with passed in context.
     *
     * @param node    A {@link ContainsPredicate}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitContainsPredicate(ContainsPredicate node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link ReferenceWithFunction} with passed in context.
     *
     * @param node    A {@link ContainsPredicate}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitReferenceWithFunction(ReferenceWithFunction node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link FunctionCall} with passed in context.
     *
     * @param node    A {@link FunctionCall}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitFunctionCall(FunctionCall node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link StringLiteral} with passed in context.
     *
     * @param node    A {@link StringLiteral}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitStringLiteral(StringLiteral node, C context) {
        return visitLiteral(node, context);
    }

    /**
     * Visit a {@link BooleanLiteral} with passed in context.
     *
     * @param node    A {@link BooleanLiteral}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitBooleanLiteral(BooleanLiteral node, C context) {
        return visitLiteral(node, context);
    }

    /**
     * Visit a {@link ValueListExpression} with passed in context.
     *
     * @param node    A {@link ValueListExpression}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitValueListExpression(ValueListExpression node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit an {@link Identifier} with passed in context.
     *
     * @param node    An {@link Identifier}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitIdentifier(Identifier node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link Distribution} with passed in context.
     *
     * @param node    A {@link Distribution}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitDistribution(Distribution node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link TopK} with passed in context.
     *
     * @param node    A {@link TopK}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitTopK(TopK node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link NullLiteral} with passed in context.
     *
     * @param node    A {@link NullLiteral}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitNullLiteral(NullLiteral node, C context) {
        return visitLiteral(node, context);
    }

    /**
     * Visit a {@link ArithmeticUnaryExpression} with passed in context.
     *
     * @param node    A {@link ArithmeticUnaryExpression}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link NotExpression} with passed in context.
     *
     * @param node    A {@link NotExpression}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitNotExpression(NotExpression node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link SelectItem} with passed in context.
     *
     * @param node    A {@link SelectItem}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitSelectItem(SelectItem node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link SingleColumn} with passed in context.
     *
     * @param node    A {@link SingleColumn}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitSingleColumn(SingleColumn node, C context) {
        return visitSelectItem(node, context);
    }

    /**
     * Visit a {@link AllColumns} with passed in context.
     *
     * @param node    A {@link AllColumns}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitAllColumns(AllColumns node, C context) {
        return visitSelectItem(node, context);
    }

    /**
     * Visit a {@link LikePredicate} with passed in context.
     *
     * @param node    A {@link LikePredicate}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitLikePredicate(LikePredicate node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link IsNotNullPredicate} with passed in context.
     *
     * @param node    A {@link IsNotNullPredicate}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link IsNullPredicate} with passed in context.
     *
     * @param node    A {@link IsNullPredicate}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitIsNullPredicate(IsNullPredicate node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link IsNotEmptyPredicate} with passed in context.
     *
     * @param node    A {@link IsNotEmptyPredicate}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitIsNotEmptyPredicate(IsNotEmptyPredicate node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link IsEmptyPredicate} with passed in context.
     *
     * @param node    A {@link IsEmptyPredicate}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitIsEmptyPredicate(IsEmptyPredicate node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link LongLiteral} with passed in context.
     *
     * @param node    A {@link LongLiteral}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitLongLiteral(LongLiteral node, C context) {
        return visitLiteral(node, context);
    }

    /**
     * Visit a {@link LogicalBinaryExpression} with passed in context.
     *
     * @param node    A {@link LogicalBinaryExpression}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context) {
        return visitExpression(node, context);
    }

    /**
     * Visit a {@link SortItem} with passed in context.
     *
     * @param node    A {@link SortItem}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitSortItem(SortItem node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link WindowInclude} with passed in context.
     *
     * @param node    A {@link WindowInclude}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitWindowInclude(WindowInclude node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link Windowing} with passed in context.
     *
     * @param node    A {@link Windowing}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitWindowing(Windowing node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link Stream} with passed in context.
     *
     * @param node    A {@link Stream}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitStream(Stream node, C context) {
        return visitQueryBody(node, context);
    }

    /**
     * Visit a {@link GroupBy} with passed in context.
     *
     * @param node    A {@link GroupBy}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitGroupBy(GroupBy node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link GroupingElement} with passed in context.
     *
     * @param node    A {@link GroupingElement}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitGroupingElement(GroupingElement node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link SimpleGroupBy} with passed in context.
     *
     * @param node    A {@link SimpleGroupBy}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitSimpleGroupBy(SimpleGroupBy node, C context) {
        return visitGroupingElement(node, context);
    }

    /**
     * Visit a {@link CastExpression} with passed in context.
     *
     * @param node    A {@link CastExpression}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitCastExpression(CastExpression node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link InfixExpression} with passed in context.
     *
     * @param node    A {@link InfixExpression}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitBinaryExpression(InfixExpression node, C context) {
        return visitNode(node, context);
    }

    /**
     * Visit a {@link ParensExpression} with passed in context.
     *
     * @param node    A {@link ParensExpression}.
     * @param context A {@link C}.
     * @return A {@link R}.
     */
    protected R visitParensExpression(ParensExpression node, C context) {
        return visitNode(node, context);
    }
}
