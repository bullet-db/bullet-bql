/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.aggregations.CountDistinct;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.yahoo.bullet.aggregations.TopK.NEW_NAME_FIELD;
import static com.yahoo.bullet.aggregations.TopK.THRESHOLD_FIELD;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.OPERATIONS;
import static com.yahoo.bullet.parsing.Aggregation.Type.COUNT_DISTINCT;
import static com.yahoo.bullet.parsing.Aggregation.Type.DISTRIBUTION;
import static com.yahoo.bullet.parsing.Aggregation.Type.GROUP;
import static com.yahoo.bullet.parsing.Aggregation.Type.RAW;
import static com.yahoo.bullet.parsing.Aggregation.Type.TOP_K;

@Slf4j
public class AggregationExtractor {
    static Aggregation extractAggregation(ProcessedQuery processedQuery) {
        switch (processedQuery.getQueryType()) {
            case SELECT:
            case SELECT_ALL:
                return extractRaw(processedQuery);
            case SELECT_DISTINCT:
                return extractDistinct(processedQuery);
            case GROUP:
                return extractGroup(processedQuery);
            case COUNT_DISTINCT:
                return extractCountDistinct(processedQuery);
            case DISTRIBUTION:
                return extractDistribution(processedQuery);
            case TOP_K:
                return extractTopK(processedQuery);
            case SPECIAL_K:
                return extractSpecialK(processedQuery);
        }
        throw new ParsingException("Unknown query type");
    }

    private static Aggregation extractRaw(ProcessedQuery processedQuery) {
        processedQuery.setAggregateMapping(new HashMap<>());
        return new Aggregation(processedQuery.getLimit(), RAW);
    }

    /*
    For SELECT DISTINCT, the GROUP BY fields are exactly the SELECT items. They're mapped as expression/field name
    to alias (if it exists; otherwise, just name). If there are non-field expressions (i.e. computations), all fields
    will be projected.
    */
    private static Aggregation extractDistinct(ProcessedQuery processedQuery) {
        Aggregation aggregation = new Aggregation(processedQuery.getLimit(), GROUP);

        List<ExpressionNode> expressions = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toList());

        processedQuery.setAggregateMapping(new HashMap<>(expressions.stream().collect(Collectors.toMap(Function.identity(), node -> new FieldExpression(processedQuery.getAliasOrName(node))))));

        aggregation.setFields(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toMap(ExpressionNode::getName, processedQuery::getAliasOrName)));

        return aggregation;
    }

    /*
    The GROUP BY fields can be given aliases if they are SELECT'd AS some alias.
    Aggregate operations with an argument take the expression by name, so these expressions will be projected into
    the record beforehand.
    */
    private static Aggregation extractGroup(ProcessedQuery processedQuery) {
        Aggregation aggregation = new Aggregation(processedQuery.getLimit(), GROUP);

        Map<ExpressionNode, Expression> mapping = new HashMap<>();

        if (!processedQuery.getGroupByNodes().isEmpty()) {

            mapping.putAll(processedQuery.getGroupByNodes().stream().collect(Collectors.toMap(Function.identity(), node -> new FieldExpression(processedQuery.getAliasOrName(node)))));

            aggregation.setFields(processedQuery.getGroupByNodes().stream().collect(Collectors.toMap(ExpressionNode::getName, processedQuery::getAliasOrName)));
        }
        List<Map<String, Object>> operations = processedQuery.getGroupOpNodes().stream().map(node -> {
                Map<String, Object> operation = new HashMap<>();
                operation.put(GroupOperation.OPERATION_TYPE, node.getOp());
                operation.put(GroupOperation.OPERATION_NEW_NAME, processedQuery.getAliasOrName(node));

                mapping.put(node, new FieldExpression(processedQuery.getAliasOrName(node)));

                if (node.getOp() != COUNT) {
                    // Use name and not alias since fields aren't renamed until after aggregation
                    operation.put(GroupOperation.OPERATION_FIELD, node.getExpression().getName());
                }
                return operation;
            }).collect(Collectors.toList());
        if (!operations.isEmpty()) {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put(OPERATIONS, operations);
            aggregation.setAttributes(attributes);
        }

        processedQuery.setAggregateMapping(mapping);

        return aggregation;
    }

    // No aliases for the COUNT DISTINCT fields since they don't show up in the returned record.
    private static Aggregation extractCountDistinct(ProcessedQuery processedQuery) {
        CountDistinctNode countDistinct = processedQuery.getCountDistinct();

        Aggregation aggregation = new Aggregation();
        aggregation.setType(COUNT_DISTINCT);
        aggregation.setFields(countDistinct.getExpressions().stream().collect(Collectors.toMap(ExpressionNode::getName, ExpressionNode::getName)));
        aggregation.setAttributes(Collections.singletonMap(CountDistinct.NEW_NAME_FIELD, processedQuery.getAliasOrName(countDistinct)));

        Map<ExpressionNode, Expression> mapping = new HashMap<>();
        mapping.put(countDistinct, new FieldExpression(processedQuery.getAliasOrName(countDistinct)));

        processedQuery.setAggregateMapping(mapping);

        return aggregation;
    }

    // No alias for the field since it doesn't show up in the returned record.
    private static Aggregation extractDistribution(ProcessedQuery processedQuery) {
        DistributionNode distribution = processedQuery.getDistribution();
        String name = distribution.getExpression().getName();

        Aggregation aggregation = new Aggregation(processedQuery.getLimit(), DISTRIBUTION);
        aggregation.setFields(Collections.singletonMap(name, name));
        aggregation.setAttributes(distribution.getAttributes());

        // shouldn't have to add anything to mapping
        processedQuery.setAggregateMapping(new HashMap<>());

        return aggregation;
    }

    // Fields get mapped to their aliases (if they exist; otherwise just their names)
    private static Aggregation extractTopK(ProcessedQuery processedQuery) {
        TopKNode topK = processedQuery.getTopK();

        Aggregation aggregation = new Aggregation(topK.getSize(), TOP_K);
        aggregation.setFields(topK.getExpressions().stream().collect(Collectors.toMap(ExpressionNode::getName, processedQuery::getAliasOrName)));


        processedQuery.setAggregateMapping(new HashMap<>(topK.getExpressions().stream().collect(Collectors.toMap(Function.identity(), node -> new FieldExpression(processedQuery.getAliasOrName(node))))));


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

    // Special K assumes the HAVING clause must be of the form COUNT(*) >= X
    private static Aggregation extractSpecialK(ProcessedQuery processedQuery) {
        Aggregation aggregation = new Aggregation(processedQuery.getLimit(), TOP_K);
        aggregation.setFields(processedQuery.getGroupByNodes().stream().collect(Collectors.toMap(ExpressionNode::getName, processedQuery::getAliasOrName)));

        Map<ExpressionNode, Expression> mapping = new HashMap<>();

        mapping.putAll(processedQuery.getGroupByNodes().stream().collect(Collectors.toMap(Function.identity(), node -> new FieldExpression(processedQuery.getAliasOrName(node)))));

        Map<String, Object> attributes = new HashMap<>();
        if (processedQuery.getHavingNode() != null) {
            attributes.put(THRESHOLD_FIELD, ((LiteralNode) ((BinaryExpressionNode) processedQuery.getHavingNode()).getRight()).getValue());
        }

        ExpressionNode countNode = processedQuery.getGroupOpNodes().iterator().next();

        attributes.put(NEW_NAME_FIELD, processedQuery.getAliasOrName(countNode));
        aggregation.setAttributes(attributes);

        mapping.put(countNode, new FieldExpression(processedQuery.getAliasOrName(countNode)));

        processedQuery.setAggregateMapping(mapping);

        return aggregation;
    }
}
