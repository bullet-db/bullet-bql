/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DefaultTraversalVisitor;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.GroupByNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.LateralViewNode;
import com.yahoo.bullet.bql.tree.Node;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SelectNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.StreamNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.WindowNode;

public class QueryProcessor extends DefaultTraversalVisitor<Void, ProcessedQuery> {
    private static final QueryProcessor INSTANCE = new QueryProcessor();
    private static final String MAX = "MAX";
    public static final String DEFAULT_TOP_K_ALIAS = "Count";

    @Override
    public Void process(Node node, ProcessedQuery processedQuery) {
        super.process(node, processedQuery);
        if (node instanceof ExpressionNode) {
            processedQuery.addExpression((ExpressionNode) node);
        }
        return null;
    }

    @Override
    protected Void visitQuery(QueryNode node, ProcessedQuery processedQuery) {
        super.visitQuery(node, processedQuery);
        processedQuery.setWhere(node.getWhere());
        processedQuery.setHaving(node.getHaving());
        if (node.getLimit() != null) {
            processedQuery.setLimit(Integer.parseInt(node.getLimit()));
        }
        if (node.getOuterQuery() != null) {
            processedQuery.setOuterQuery(visit(node.getOuterQuery()));
        }
        return null;
    }

    @Override
    protected Void visitSelect(SelectNode node, ProcessedQuery processedQuery) {
        if (node.isDistinct()) {
            processedQuery.addQueryType(ProcessedQuery.QueryType.SELECT_DISTINCT);
        }
        return super.visitSelect(node, processedQuery);
    }

    @Override
    protected Void visitSelectItem(SelectItemNode node, ProcessedQuery processedQuery) {
        super.visitSelectItem(node, processedQuery);
        ExpressionNode expression = node.getExpression();
        if (expression != null) {
            processedQuery.addSelectNode(expression);
            if (node.getAlias() != null) {
                processedQuery.addAlias(expression, node.getAlias().getValue());
            } else if (expression instanceof TopKNode) {
                processedQuery.addAlias(expression, DEFAULT_TOP_K_ALIAS);
            }
        } else {
            processedQuery.addQueryType(ProcessedQuery.QueryType.SELECT_ALL);
        }
        return null;
    }

    @Override
    protected Void visitStream(StreamNode node, ProcessedQuery processedQuery) {
        String timeDuration = node.getTimeDuration();
        if (timeDuration != null) {
            processedQuery.setDuration(timeDuration.equalsIgnoreCase(MAX) ? Long.MAX_VALUE : Long.parseLong(timeDuration));
        }
        return null;
    }

    @Override
    protected Void visitLateralView(LateralViewNode node, ProcessedQuery processedQuery) {
        super.visitLateralView(node, processedQuery);
        processedQuery.setLateralView(node);
        return null;
    }

    @Override
    protected Void visitGroupBy(GroupByNode node, ProcessedQuery processedQuery) {
        super.visitGroupBy(node, processedQuery);
        processedQuery.addGroupByNodes(node.getExpressions());
        return null;
    }

    @Override
    protected Void visitSortItem(SortItemNode node, ProcessedQuery processedQuery) {
        super.visitSortItem(node, processedQuery);
        processedQuery.addSortItemNode(node);
        return null;
    }

    @Override
    protected Void visitWindow(WindowNode node, ProcessedQuery processedQuery) {
        processedQuery.setWindow(node);
        return null;
    }

    @Override
    protected Void visitExpression(ExpressionNode node, ProcessedQuery processedQuery) {
        return null;
    }

    @Override
    protected Void visitGroupOperation(GroupOperationNode node, ProcessedQuery processedQuery) {
        super.visitGroupOperation(node, processedQuery);
        processedQuery.addGroupOpNode(node);
        processedQuery.addAggregate(node);
        return null;
    }

    @Override
    protected Void visitCountDistinct(CountDistinctNode node, ProcessedQuery processedQuery) {
        super.visitCountDistinct(node, processedQuery);
        processedQuery.setCountDistinct(node);
        processedQuery.addAggregate(node);
        return null;
    }

    @Override
    protected Void visitDistribution(DistributionNode node, ProcessedQuery processedQuery) {
        super.visitDistribution(node, processedQuery);
        processedQuery.setDistribution(node);
        processedQuery.addAggregate(node);
        return null;
    }

    @Override
    protected Void visitTopK(TopKNode node, ProcessedQuery processedQuery) {
        super.visitTopK(node, processedQuery);
        processedQuery.setTopK(node);
        processedQuery.addAggregate(node);
        return null;
    }

    public static ProcessedQuery visit(Node node) {
        ProcessedQuery processedQuery = new ProcessedQuery();
        INSTANCE.process(node, processedQuery);
        return processedQuery;
    }
}
