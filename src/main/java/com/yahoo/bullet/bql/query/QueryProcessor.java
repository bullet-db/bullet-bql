/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.CastExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupByNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.ListExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.NAryExpressionNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.NullPredicateNode;
import com.yahoo.bullet.bql.tree.OrderByNode;
import com.yahoo.bullet.bql.tree.ParenthesesExpressionNode;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SelectNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.StreamNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.UnaryExpressionNode;
import com.yahoo.bullet.bql.tree.WindowNode;

import java.util.Arrays;

public class QueryProcessor extends DefaultTraversalVisitor<ProcessedQuery, ProcessedQuery> {
    @Override
    public ProcessedQuery process(Node node) {
        return process(node, new ProcessedQuery());
    }

    @Override
    protected ProcessedQuery visitNode(Node node, ProcessedQuery context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected ProcessedQuery visitQuery(QueryNode node, ProcessedQuery context) {
        super.visitQuery(node, context);
        context.setWhereNode(node.getWhere());
        context.setHavingNode(node.getHaving());
        if (node.getLimit() != null) {
            context.setLimit(Integer.parseInt(node.getLimit()));
        }
        if (context.getQueryTypeSet().isEmpty()) {
            context.getQueryTypeSet().add(ProcessedQuery.QueryType.SELECT);
        }
        return context.validate();
    }

    @Override
    protected ProcessedQuery visitSelect(SelectNode node, ProcessedQuery context) {
        if (node.isDistinct()) {
            context.getQueryTypeSet().add(ProcessedQuery.QueryType.SELECT_DISTINCT);
        }
        return super.visitSelect(node, context);
    }

    @Override
    protected ProcessedQuery visitSelectItem(SelectItemNode node, ProcessedQuery context) {
        super.visitSelectItem(node, context);
        if (node.getExpression() != null) {
            context.getSelectNodes().add(node);
            if (node.getAlias() != null) {
                context.getAliases().put(node.getExpression(), node.getAlias().getValue());
            }
        } else {
            context.getQueryTypeSet().add(ProcessedQuery.QueryType.SELECT_ALL);
        }
        return context;
    }

    @Override
    protected ProcessedQuery visitStream(StreamNode node, ProcessedQuery context) {
        String timeDuration = node.getTimeDuration();
        if (timeDuration != null) {
            context.setTimeDuration(timeDuration.equalsIgnoreCase("MAX") ? Long.MAX_VALUE : Long.parseLong(timeDuration));
        }
        return context;
    }

    @Override
    protected ProcessedQuery visitGroupBy(GroupByNode node, ProcessedQuery context) {
        super.visitGroupBy(node, context);
        context.getGroupByNodes().addAll(node.getExpressions());
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.GROUP);
        return context;
    }

    @Override
    protected ProcessedQuery visitOrderBy(OrderByNode node, ProcessedQuery context) {
        return super.visitOrderBy(node, context);
    }

    @Override
    protected ProcessedQuery visitSortItem(SortItemNode node, ProcessedQuery context) {
        super.visitSortItem(node, context);
        context.getOrderByNodes().add(node);
        return context;
    }

    @Override
    protected ProcessedQuery visitWindow(WindowNode node, ProcessedQuery context) {
        context.setWindow(node);
        return context;
    }

    @Override
    protected ProcessedQuery visitExpression(ExpressionNode node, ProcessedQuery context) {
        throw new RuntimeException("This method should not be called.");
    }

    @Override
    protected ProcessedQuery visitFieldExpression(FieldExpressionNode node, ProcessedQuery context) {
        return context;
    }

    @Override
    protected ProcessedQuery visitListExpression(ListExpressionNode node, ProcessedQuery context) {
        super.visitListExpression(node, context);

        context.addExpression(node, node.getExpressions());

        return context;
    }

    @Override
    protected ProcessedQuery visitNullPredicate(NullPredicateNode node, ProcessedQuery context) {
        super.visitNullPredicate(node, context);

        context.addExpression(node, node.getExpression());

        return context;
    }

    @Override
    protected ProcessedQuery visitUnaryExpression(UnaryExpressionNode node, ProcessedQuery context) {
        super.visitUnaryExpression(node, context);

        context.addExpression(node, node.getExpression());

        return context;
    }

    @Override
    protected ProcessedQuery visitNAryExpression(NAryExpressionNode node, ProcessedQuery context) {
        super.visitNAryExpression(node, context);

        context.addExpression(node, node.getExpressions());

        return context;
    }

    @Override
    protected ProcessedQuery visitGroupOperation(GroupOperationNode node, ProcessedQuery context) {
        super.visitGroupOperation(node, context);

        context.addExpression(node, node.getExpression());

        context.getAggregateNodes().add(node);
        context.getGroupOpNodes().add(node);
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.GROUP);

        return context;
    }

    @Override
    protected ProcessedQuery visitCountDistinct(CountDistinctNode node, ProcessedQuery context) {
        super.visitCountDistinct(node, context);

        context.addExpression(node, node.getExpressions());

        context.getAggregateNodes().add(node);
        context.getCountDistinctNodes().add(node);
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.COUNT_DISTINCT);

        return context;
    }

    @Override
    protected ProcessedQuery visitDistribution(DistributionNode node, ProcessedQuery context) {
        super.visitDistribution(node, context);

        context.addExpression(node, node.getExpression());

        context.getAggregateNodes().add(node);
        context.getDistributionNodes().add(node);
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.DISTRIBUTION);

        return context;
    }

    @Override
    protected ProcessedQuery visitTopK(TopKNode node, ProcessedQuery context) {
        super.visitTopK(node, context);

        context.addExpression(node, node.getExpressions());

        context.getAggregateNodes().add(node);
        context.getTopKNodes().add(node);
        context.getQueryTypeSet().add(ProcessedQuery.QueryType.TOP_K);

        return context;
    }

    @Override
    protected ProcessedQuery visitCastExpression(CastExpressionNode node, ProcessedQuery context) {
        super.visitCastExpression(node, context);

        context.addExpression(node, node.getExpression());

        return context;
    }

    @Override
    protected ProcessedQuery visitBinaryExpression(BinaryExpressionNode node, ProcessedQuery context) {
        super.visitBinaryExpression(node, context);

        context.addExpression(node, Arrays.asList(node.getLeft(), node.getRight()));

        return context;
    }

    @Override
    protected ProcessedQuery visitParenthesesExpression(ParenthesesExpressionNode node, ProcessedQuery context) {
        super.visitParenthesesExpression(node, context);

        context.addExpression(node, node.getExpression());

        return context;
    }

    @Override
    protected ProcessedQuery visitLiteral(LiteralNode node, ProcessedQuery context) {
        return context;
    }
}
