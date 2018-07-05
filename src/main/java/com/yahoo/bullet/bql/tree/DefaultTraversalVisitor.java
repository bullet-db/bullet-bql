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

public abstract class DefaultTraversalVisitor<R, C> extends ASTVisitor<R, C> {
    @Override
    protected R visitBetweenPredicate(BetweenPredicate node, C context) {
        process(node.getValue(), context);
        process(node.getMin(), context);
        process(node.getMax(), context);

        return null;
    }

    @Override
    protected R visitComparisonExpression(ComparisonExpression node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitQuery(Query node, C context) {
        if (node.getWith().isPresent()) {
            process(node.getWith().get(), context);
        }
        process(node.getQueryBody(), context);
        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }

        return null;
    }

    @Override
    protected R visitSelect(Select node, C context) {
        for (SelectItem item : node.getSelectItems()) {
            process(item, context);
        }

        return null;
    }

    @Override
    protected R visitSingleColumn(SingleColumn node, C context) {
        process(node.getExpression(), context);

        return null;
    }

    @Override
    protected R visitInPredicate(InPredicate node, C context) {
        process(node.getValue(), context);
        process(node.getValueList(), context);

        return null;
    }

    @Override
    protected R visitFunctionCall(FunctionCall node, C context) {
        for (Expression argument : node.getArguments()) {
            process(argument, context);
        }

        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }

        if (node.getFilter().isPresent()) {
            process(node.getFilter().get(), context);
        }

        return null;
    }

    @Override
    protected R visitDereferenceExpression(DereferenceExpression node, C context) {
        process(node.getBase(), context);
        return null;
    }

    @Override
    protected R visitDistribution(Distribution node, C context) {
        for (Expression value : node.getColumns()) {
            process(value, context);
        }

        return null;
    }

    @Override
    protected R visitTopK(TopK node, C context) {
        for (Expression value : node.getColumns()) {
            process(value, context);
        }

        return null;
    }

    @Override
    protected R visitInListExpression(InListExpression node, C context) {
        for (Expression value : node.getValues()) {
            process(value, context);
        }

        return null;
    }

    @Override
    protected R visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitNotExpression(NotExpression node, C context) {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitLikePredicate(LikePredicate node, C context) {
        process(node.getValue(), context);
        process(node.getPatterns(), context);
        if (node.getEscape().isPresent()) {
            process(node.getEscape().get(), context);
        }

        return null;
    }

    @Override
    protected R visitLikeListExpression(LikeListExpression node, C context) {
        for (Expression value : node.getValues()) {
            process(value, context);
        }

        return null;
    }

    @Override
    protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitIsNullPredicate(IsNullPredicate node, C context) {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitIsNotEmptyPredicate(IsNotEmptyPredicate node, C context) {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitIsEmptyPredicate(IsEmptyPredicate node, C context) {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitQuerySpecification(QuerySpecification node, C context) {
        process(node.getSelect(), context);
        if (node.getFrom().isPresent()) {
            process(node.getFrom().get(), context);
        }
        if (node.getWhere().isPresent()) {
            process(node.getWhere().get(), context);
        }
        if (node.getGroupBy().isPresent()) {
            process(node.getGroupBy().get(), context);
        }
        if (node.getHaving().isPresent()) {
            process(node.getHaving().get(), context);
        }
        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }
        if (node.getWindowing().isPresent()) {
            process(node.getWindowing().get(), context);
        }
        return null;
    }

    @Override
    protected R visitWindowing(Windowing node, C context) {
        process(node.getInclude(), context);

        return null;
    }

    @Override
    protected R visitGroupBy(GroupBy node, C context) {
        for (GroupingElement groupingElement : node.getGroupingElements()) {
            process(groupingElement, context);
        }

        return null;
    }

    @Override
    protected R visitSimpleGroupBy(SimpleGroupBy node, C context) {
        for (Expression expression : node.getColumnExpressions()) {
            process(expression, context);
        }

        return null;
    }
}
