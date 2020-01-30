/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.processor.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;

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
                return extractTopK(processedQuery);
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
                                                                             .collect(Collectors.toSet());
        orderByFields.removeAll(getSelectExpressionNames(processedQuery));
        return orderByFields;
    }

    private static Set<String> extractAll(ProcessedQuery processedQuery) {
        return getDisjointedComplexOrderByFields(processedQuery);
    }

    // Remove every order by field that's not a select field, but don't remove simple field expressions.
    private static Set<String> extractDistinct(ProcessedQuery processedQuery) {
        return getDisjointedComplexOrderByFields(processedQuery);
    }

    // Remove every field that's not a select field.
    private static Set<String> extractAggregate(ProcessedQuery processedQuery) {
        Set<String> transientFields =
                Stream.concat(Stream.concat(processedQuery.getGroupByNodes().stream(),
                                            processedQuery.getAggregateNodes().stream()),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
                      .map(processedQuery::getAliasOrName)
                      .collect(Collectors.toSet());
        transientFields.removeAll(getSelectExpressionNames(processedQuery));
        return transientFields;
    }

    private static Set<String> extractDistribution(ProcessedQuery processedQuery) {
        return getDisjointedComplexOrderByFields(processedQuery);
    }

    private static Set<String> extractTopK(ProcessedQuery processedQuery) {
        return getDisjointedComplexOrderByFields(processedQuery);
    }

    // Remove every order by field that's not a select field, but don't remove simple field expressions.
    private static Set<String> getDisjointedComplexOrderByFields(ProcessedQuery processedQuery) {
        Set<String> orderByFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                             .filter(processedQuery::isNotSimpleFieldExpression)
                                                                             .map(processedQuery::getAliasOrName)
                                                                             .collect(Collectors.toSet());
        orderByFields.removeAll(getSelectExpressionNames(processedQuery));
        return orderByFields;
    }

    private static Set<String> getSelectExpressionNames(ProcessedQuery processedQuery) {
        return processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                       .map(processedQuery::getAliasOrName)
                                                       .collect(Collectors.toSet());
    }
}
