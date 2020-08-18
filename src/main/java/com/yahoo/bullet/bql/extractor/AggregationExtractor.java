/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ComputableProcessor;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.aggregations.CountDistinct;
import com.yahoo.bullet.query.aggregations.GroupAll;
import com.yahoo.bullet.query.aggregations.GroupBy;
import com.yahoo.bullet.query.aggregations.Raw;
import com.yahoo.bullet.query.aggregations.TopK;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.yahoo.bullet.querying.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;

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

    // If there is a projection, add a mapping for simple fields that were selected with aliases.
    private static Aggregation extractRaw(ProcessedQuery processedQuery) {
        if (processedQuery.getProjection() != null) {
            addSimplePostAggregationMapping(processedQuery, processedQuery.getProjection());
        }
        return new Raw(processedQuery.getLimit());
    }

    /*
    For SELECT DISTINCT, the GROUP BY fields are exactly the SELECT items. They're mapped as expression/field name
    to alias (if it exists; otherwise, just name). If there are non-field expressions (i.e. computations), all fields
    will have been projected.
    */
    private static Aggregation extractDistinct(ProcessedQuery processedQuery) {
        addSimplePostAggregationMapping(processedQuery, processedQuery.getSelectNodes());
        addComplexPostAggregationMapping(processedQuery, processedQuery.getSelectNodes());
        return new GroupBy(processedQuery.getLimit(), toAliasedFields(processedQuery, processedQuery.getSelectNodes()), Collections.emptySet());
    }

    /*
    The GROUP BY fields can be given aliases if they are SELECT'd AS some alias.
    Aggregate operations with an argument take the expression by name, so these expressions will be projected into
    the record beforehand.
    */
    private static Aggregation extractGroup(ProcessedQuery processedQuery) {
        Aggregation aggregation;
        Set<GroupOperation> operations = processedQuery.getGroupOpNodes().stream().map(node -> {
            String field = node.getOp() != COUNT ? node.getExpression().getName() : null;
            return new GroupOperation(node.getOp(), field, processedQuery.getAliasOrName(node));
        }).collect(Collectors.toSet());
        if (!processedQuery.getGroupByNodes().isEmpty()) {
            aggregation = new GroupBy(processedQuery.getLimit(), toAliasedFields(processedQuery, processedQuery.getGroupByNodes()), operations);
        } else {
            aggregation = new GroupAll(operations);
        }
        addSimplePostAggregationMapping(processedQuery, processedQuery.getGroupByNodes());
        addComplexPostAggregationMapping(processedQuery, processedQuery.getGroupByNodes());
        addPostAggregationMapping(processedQuery, processedQuery.getGroupOpNodes());
        return aggregation;
    }

    // No aliases for the COUNT DISTINCT fields since they don't show up in the returned record.
    private static Aggregation extractCountDistinct(ProcessedQuery processedQuery) {
        CountDistinctNode countDistinct = processedQuery.getCountDistinct();
        List<String> fields = countDistinct.getExpressions().stream().map(ExpressionNode::getName).collect(Collectors.toCollection(ArrayList::new));
        CountDistinct aggregation = new CountDistinct(fields, processedQuery.getAliasOrName(countDistinct));
        addPostAggregationMapping(processedQuery, countDistinct);
        return aggregation;
    }

    // No alias for the field since it doesn't show up in the returned record.
    private static Aggregation extractDistribution(ProcessedQuery processedQuery) {
        return processedQuery.getDistribution().getAggregation(processedQuery.getLimit());
    }

    // Fields get mapped to their aliases (if they exist; otherwise just their names). Top K is given an alias by default.
    private static Aggregation extractTopK(ProcessedQuery processedQuery) {
        TopKNode topK = processedQuery.getTopK();
        Map<String, String> fields = toAliasedFields(processedQuery, topK.getExpressions());
        addSimplePostAggregationMapping(processedQuery, topK.getExpressions());
        addComplexPostAggregationMapping(processedQuery, topK.getExpressions());
        return new TopK(fields, topK.getSize(), topK.getThreshold(), processedQuery.getAlias(topK));
    }

    // Special K assumes the HAVING clause must be of the form COUNT(*) >= X
    private static Aggregation extractSpecialK(ProcessedQuery processedQuery) {
        ExpressionNode countNode = processedQuery.getGroupOpNodes().iterator().next();
        Map<String, String> fields = toAliasedFields(processedQuery, processedQuery.getGroupByNodes());
        Long threshold = null;
        if (processedQuery.getHavingNode() != null) {
            threshold = ((Number) ((LiteralNode) ((BinaryExpressionNode) processedQuery.getHavingNode()).getRight()).getValue()).longValue();
        }
        addSimplePostAggregationMapping(processedQuery, processedQuery.getGroupByNodes());
        addComplexPostAggregationMapping(processedQuery, processedQuery.getGroupByNodes());
        addPostAggregationMapping(processedQuery, countNode);
        return new TopK(fields, processedQuery.getLimit(), threshold, processedQuery.getAliasOrName(countNode));
    }

    private static Map<String, String> toAliasedFields(ProcessedQuery processedQuery, Collection<ExpressionNode> expressions) {
        return expressions.stream().collect(Collectors.toMap(ExpressionNode::getName, processedQuery::getAliasOrName, throwingMerger(), HashMap::new));
    }

    private static <T> BinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        };
    }

    // Aggregates must be mapped to a field expression after postaggregation so that it is possible to actually access their values in the record.
    private static void addPostAggregationMapping(ProcessedQuery processedQuery, Collection<? extends ExpressionNode> expressions) {
        Map<ExpressionNode, Expression> mapping = processedQuery.getPostAggregationMapping();
        expressions.forEach(node -> mapping.put(node, new FieldExpression(processedQuery.getAliasOrName(node))));
    }

    // Aggregates must be mapped to a field expression after postaggregation so that it is possible to actually access their values in the record.
    private static void addPostAggregationMapping(ProcessedQuery processedQuery, ExpressionNode expression) {
        processedQuery.getPostAggregationMapping().put(expression, new FieldExpression(processedQuery.getAliasOrName(expression)));
    }

    /*
    Add a mapping for any simple field with an alias to redirect that simple field to the alias in postaggregations if
    the field's name is not covered by any other aliases.
    For example, for "abc AS def", usage of the field "abc" in postaggregations gets mapped to "def" as long as "abc"
    itself is not another alias like "abc + 5 AS abc".
    */
    private static void addSimplePostAggregationMapping(ProcessedQuery processedQuery, Collection<ExpressionNode> expressions) {
        Map<ExpressionNode, Expression> mapping = processedQuery.getPostAggregationMapping();
        expressions.stream()
                   .filter(processedQuery::isSimpleFieldExpression)
                   .filter(processedQuery::hasAlias)
                   .filter(node -> !processedQuery.isAlias(node.getName()))
                   .forEach(node -> mapping.put(node, new FieldExpression(processedQuery.getAliasOrName(node))));
    }

    // Add a mapping for any field that is not computable from the projected simple fields.
    private static void addComplexPostAggregationMapping(ProcessedQuery processedQuery, Collection<ExpressionNode> expressions) {
        Map<ExpressionNode, Expression> mapping = processedQuery.getPostAggregationMapping();
        processedQuery.setSelectNames(Stream.concat(expressions.stream().filter(processedQuery::isSimpleFieldExpression).map(ExpressionNode::getName),
                                                    processedQuery.getAliases().values().stream())
                                            .collect(Collectors.toSet()));
        ComputableProcessor.visit(expressions, processedQuery).forEach(node  -> mapping.put(node, new FieldExpression(processedQuery.getAliasOrName(node))));
    }
}
