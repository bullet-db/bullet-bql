/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransientFieldExtractor {
    static Set<String> extractTransientFields(ProcessedQuery processedQuery) {
        switch (processedQuery.getQueryType()) {
            case SELECT:
                return extractSelect(processedQuery);
            case SELECT_ALL:
                return extractAll(processedQuery);
            case SELECT_DISTINCT:
                return extractDistinct(processedQuery);
            case GROUP:
            case COUNT_DISTINCT:
                return extractAggregate(processedQuery);
            case DISTRIBUTION:
                return extractDistribution(processedQuery);
            case TOP_K:
                // Doesn't have transient fields since can't be a value and order by is not allowed
            case SPECIAL_K:
                // Doesn't have transient fields since selected fields and order by fields are fixed
                return null;
        }
        throw new ParsingException("Unknown query type");
    }

    // Remove every order by field that's not a select field.
    private static Set<String> extractSelect(ProcessedQuery processedQuery) {
        Set<String> orderByFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                             .map(processedQuery::getAliasOrName)
                                                                             .collect(Collectors.toCollection(HashSet::new));
        orderByFields.removeAll(getSelectFields(processedQuery));
        return orderByFields;
    }

    private static Set<String> extractAll(ProcessedQuery processedQuery) {
        /*
        A SELECT_ALL PASS_THROUGH query only has simple fields, so it will never have a culling post-aggregation.
        Without this check, this method should still return an empty set and therefore result in no culling, but this is
        a simpler guarantee of correctness.
        */
        if (processedQuery.getProjection() == null) {
            return null;
        }
        return getNonSelectOrComplexOrderByFields(processedQuery);
    }

    // Remove every order by field that's not a select field, but don't remove simple field expressions.
    private static Set<String> extractDistinct(ProcessedQuery processedQuery) {
        return getNonSelectOrComplexOrderByFields(processedQuery);
    }

    // Remove every field that's not a select field.
    private static Set<String> extractAggregate(ProcessedQuery processedQuery) {
        Set<String> transientFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                               .map(processedQuery::getAliasOrName)
                                                                               .collect(Collectors.toCollection(HashSet::new));
        transientFields.removeAll(getSelectFields(processedQuery));
        Set<ExpressionNode> aggregateNodes = Stream.concat(processedQuery.getGroupByNodes().stream(),
                                                           processedQuery.getAggregateNodes().stream()).collect(Collectors.toCollection(HashSet::new));
        aggregateNodes.removeAll(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toSet()));
        transientFields.addAll(aggregateNodes.stream().map(processedQuery::getAliasOrName).collect(Collectors.toSet()));
        return transientFields;
    }

    private static Set<String> extractDistribution(ProcessedQuery processedQuery) {
        return getNonSelectOrComplexOrderByFields(processedQuery);
    }

    // Remove every order by field that's not a select field, but don't remove simple field expressions.
    private static Set<String> getNonSelectOrComplexOrderByFields(ProcessedQuery processedQuery) {
        Set<String> orderByFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                             .filter(processedQuery::isNotSimpleFieldExpression)
                                                                             .map(processedQuery::getAliasOrName)
                                                                             .collect(Collectors.toCollection(HashSet::new));
        orderByFields.removeAll(getSelectFields(processedQuery));
        return orderByFields;
    }

    private static Set<String> getSelectFields(ProcessedQuery processedQuery) {
        return processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                       .map(processedQuery::getAliasOrName)
                                                       .collect(Collectors.toSet());
    }
}
