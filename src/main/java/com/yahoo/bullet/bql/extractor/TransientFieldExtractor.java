/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.ExpressionNode;

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
            case GROUP:
            case COUNT_DISTINCT:
                return extractAggregate(processedQuery);
            case SELECT_DISTINCT:
            case DISTRIBUTION:
            case TOP_K:
            case SPECIAL_K:
                /*
                SELECT DISTINCT and SPECIAL K do not have transient fields since all fields are from the SELECT clause.
                DISTRIBUTION and TOP K do not have transient fields since they cannot be used in a computation and
                therefore must be directly in the SELECT clause.
                */
                return null;
        }
        throw new ParsingException("Unknown query type");
    }

    // Remove every extra order by field because none of them are selected.
    private static Set<String> extractSelect(ProcessedQuery processedQuery) {
        return processedQuery.getOrderByExtraSelectNodes().stream().map(ExpressionNode::getName).collect(Collectors.toCollection(HashSet::new));
    }

    private static Set<String> extractAll(ProcessedQuery processedQuery) {
        /*
        A SELECT_ALL PASS_THROUGH query only has simple fields, so it will never have a culling post-aggregation.
        Without this check, this method should still return an empty set and therefore result in no culling, but this is
        a simpler and safer guarantee of correctness.
        */
        if (processedQuery.getProjection() == null) {
            return null;
        }
        // Cull the select fields that were aliased unless they themselves are an alias.
        return processedQuery.getSelectNodes().stream().filter(processedQuery::isSimpleFieldExpression)
                                                       .filter(processedQuery::hasAlias)
                                                       .map(ExpressionNode::getName)
                                                       .filter(name -> !processedQuery.isAlias(name))
                                                       .collect(Collectors.toSet());
    }

    // Remove every field that's not a select field.
    private static Set<String> extractAggregate(ProcessedQuery processedQuery) {
        Set<String> fields = Stream.concat(processedQuery.getGroupByNodes().stream(),
                                           processedQuery.getAggregateNodes().stream())
                                   .map(processedQuery::getAliasOrName)
                                   .collect(Collectors.toCollection(HashSet::new));
        processedQuery.getSelectNodes().stream().map(processedQuery::getAliasOrName).forEach(fields::remove);
        return fields;
    }
}
