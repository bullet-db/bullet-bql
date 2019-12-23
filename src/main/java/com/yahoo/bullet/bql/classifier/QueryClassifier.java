/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.classifier;

import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.parsing.expressions.Operation;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.bql.classifier.ProcessedQuery.QueryType;

public class QueryClassifier {
    /**
     *
     *
     * @param query
     */
    public void classifyQuery(ProcessedQuery query) {
        if (isSpecialK(query)) {
            query.setQueryTypeSet(Collections.singleton(QueryType.SPECIAL_K));
        }
    }

    private static boolean isSpecialK(ProcessedQuery query) {
        if (query.getQueryType() != QueryType.GROUP) {
            return false;
        }
        if (query.getGroupByNodes().isEmpty()) {
            return false;
        }
        if (query.getLimit() == null) {
            return false;
        }
        if (query.getGroupOpNodes().size() != 1) {
            return false;
        }
        GroupOperationNode groupOperationNode = query.getGroupOpNodes().iterator().next();
        if (groupOperationNode.getOp() != COUNT) {
            return false;
        }
        Set<ExpressionNode> selectNodes = query.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toSet());
        if (!selectNodes.contains(groupOperationNode)) {
            return false;
        }
        if (!selectNodes.containsAll(query.getGroupByNodes())) {
            return false;
        }
        if (query.getOrderByNodes().size() != 1) {
            return false;
        }
        // Compare by expression since both should point to the same field expression
        Expression groupOperationExpression = query.getExpression(groupOperationNode);
        SortItemNode sortItemNode = query.getOrderByNodes().get(0);
        if (!query.getExpression(sortItemNode.getExpression()).equals(groupOperationExpression) || sortItemNode.getOrdering() != SortItemNode.Ordering.DESCENDING) {
            return false;
        }
        if (query.getHavingNode() != null) {
            if (!(query.getHavingNode() instanceof BinaryExpressionNode)) {
                return false;
            }
            BinaryExpressionNode having = (BinaryExpressionNode) query.getHavingNode();
            if (!query.getExpression(having.getLeft()).equals(groupOperationExpression)) {
                return false;
            }
            if (having.getOp() != Operation.GREATER_THAN_OR_EQUALS) {
                return false;
            }
            if (!(having.getRight() instanceof LiteralNode && ((LiteralNode) having.getRight()).getValue() instanceof Number)) {
                return false;
            }
        }
        return true;
    }
}
