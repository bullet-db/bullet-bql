/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.classifier.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.LongLiteralNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.parsing.Aggregation;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.yahoo.bullet.aggregations.TopK.NEW_NAME_FIELD;
import static com.yahoo.bullet.aggregations.TopK.THRESHOLD_FIELD;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.OPERATIONS;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.OPERATION_FIELD;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.OPERATION_TYPE;
import static com.yahoo.bullet.parsing.Aggregation.Type.COUNT_DISTINCT;
import static com.yahoo.bullet.parsing.Aggregation.Type.DISTRIBUTION;
import static com.yahoo.bullet.parsing.Aggregation.Type.GROUP;
import static com.yahoo.bullet.parsing.Aggregation.Type.RAW;
import static com.yahoo.bullet.parsing.Aggregation.Type.TOP_K;

@Slf4j
public class AggregationExtractor {
    private ProcessedQuery processedQuery;

    public Aggregation extractAggregation(ProcessedQuery processedQuery) {
        this.processedQuery = processedQuery;
        switch (processedQuery.getQueryType()) {
            case SELECT:
            case SELECT_ALL:
                return extractRaw();
            case SELECT_DISTINCT:
                return extractDistinct();
            case GROUP:
                return extractGroup();
            case COUNT_DISTINCT:
                return extractCountDistinct();
            case DISTRIBUTION:
                return extractDistribution();
            case TOP_K:
                return extractTopK();
            case SPECIAL_K:
                return extractSpecialK();
        }
        throw new ParsingException("Unsupported");
    }

    private Aggregation extractRaw() {
        return new Aggregation(processedQuery.getLimit(), RAW);
    }

    private Aggregation extractDistinct() {
        Aggregation aggregation = new Aggregation(processedQuery.getLimit(), GROUP);
        aggregation.setFields(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toMap(ExpressionNode::toFormatlessString, processedQuery::getAliasOrName)));
        return aggregation;
    }

    private Aggregation extractGroup() {
        Aggregation aggregation = new Aggregation(processedQuery.getLimit(), GROUP);
        if (!processedQuery.getGroupByNodes().isEmpty()) {
            aggregation.setFields(processedQuery.getGroupByNodes().stream().collect(Collectors.toMap(ExpressionNode::toFormatlessString, processedQuery::getAliasOrName)));
        }
        List<Map<String, Object>> operations = processedQuery.getGroupOpNodes().stream().map(node -> {
            Map<String, Object> operation = new HashMap<>();
            operation.put(OPERATION_TYPE, node.getOp());
            operation.put(NEW_NAME_FIELD, processedQuery.getAlias(node));
            if (node.getOp() != COUNT) {
                // TODO this is going to be a bug
                operation.put(OPERATION_FIELD, processedQuery.getAliasOrName(node.getExpression()));
            }
            return operation;
        }).collect(Collectors.toList());
        if (!operations.isEmpty()) {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put(OPERATIONS, operations);
            aggregation.setAttributes(attributes);
        }
        return aggregation;
    }

    private Aggregation extractCountDistinct() {
        CountDistinctNode countDistinct = processedQuery.getCountDistinct();

        Aggregation aggregation = new Aggregation();
        aggregation.setType(COUNT_DISTINCT);
        aggregation.setFields(countDistinct.getExpressions().stream().collect(Collectors.toMap(ExpressionNode::toFormatlessString, ExpressionNode::toFormatlessString)));
        String alias = processedQuery.getAlias(countDistinct);
        if (alias != null) {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put(NEW_NAME_FIELD, alias);
            aggregation.setAttributes(attributes);
        }
        return aggregation;
    }

    private Aggregation extractDistribution() {
        DistributionNode distribution = processedQuery.getDistribution();
        String name = distribution.getExpression().toFormatlessString();

        Aggregation aggregation = new Aggregation(processedQuery.getLimit(), DISTRIBUTION);
        aggregation.setFields(new HashMap<>());
        aggregation.getFields().put(name, name);
        aggregation.setAttributes(distribution.getAttributes());
        return aggregation;
    }

    private Aggregation extractTopK() {
        TopKNode topK = processedQuery.getTopK();

        Aggregation aggregation = new Aggregation(topK.getSize(), TOP_K);
        aggregation.setFields(topK.getExpressions().stream().collect(Collectors.toMap(ExpressionNode::toFormatlessString, ExpressionNode::toFormatlessString)));
        Map<String, Object> attributes = new HashMap<>();
        if (topK.getThreshold() != null) {
            attributes.put(THRESHOLD_FIELD, topK.getThreshold());
        }
        String alias = processedQuery.getAlias(topK);
        if (alias != null) {
            attributes.put(NEW_NAME_FIELD, alias);
        }
        if (!attributes.isEmpty()) {
            aggregation.setAttributes(attributes);
        }
        return aggregation;
    }

    private Aggregation extractSpecialK() {
        Aggregation aggregation = new Aggregation(processedQuery.getLimit(), TOP_K);
        aggregation.setFields(processedQuery.getGroupByNodes().stream().collect(Collectors.toMap(ExpressionNode::toFormatlessString, processedQuery::getAliasOrName)));
        Map<String, Object> attributes = new HashMap<>();
        if (processedQuery.getHavingNode() != null) {
            attributes.put(THRESHOLD_FIELD, ((LongLiteralNode) ((BinaryExpressionNode) processedQuery.getHavingNode()).getRight()).getValue());
        }
        String alias = processedQuery.getAlias(processedQuery.getGroupOpNodes().iterator().next());
        if (alias != null) {
            attributes.put(NEW_NAME_FIELD, alias);
        }
        if (!attributes.isEmpty()) {
            aggregation.setAttributes(attributes);
        }
        return aggregation;
    }
}
