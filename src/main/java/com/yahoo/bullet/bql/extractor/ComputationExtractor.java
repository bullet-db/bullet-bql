/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ExpressionProcessor;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.postaggregations.Computation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ComputationExtractor {
    static Computation extractComputation(ProcessedQuery processedQuery) {
        switch (processedQuery.getQueryType()) {
            case SELECT:
            case SELECT_ALL:
            case SELECT_DISTINCT:
                // No computations since everything is handled in the initial projection.
                return null;
            case GROUP:
            case COUNT_DISTINCT:
            case DISTRIBUTION:
                return extractAggregate(processedQuery);
            case TOP_K:
                return extractTopK(processedQuery);
            case SPECIAL_K:
                return extractSpecialK(processedQuery);
        }
        throw new ParsingException("Unknown query type");
    }

    /*
    For aggregates, we need to consider the computations in SELECT. Ignore any clauses that are just simple fields,
    GROUP BY fields, or aggregates. (GROUP BY fields and aggregates are simple fields post-aggregation).
    */
    private static Computation extractAggregate(ProcessedQuery processedQuery) {
        List<ExpressionNode> expressions = processedQuery.getSelectNodes().stream()
                                                                          .filter(processedQuery::isNotGroupByNode)
                                                                          .filter(processedQuery::isNotSimpleFieldExpression)
                                                                          .filter(processedQuery::isNotAggregate)
                                                                          .distinct()
                                                                          .collect(Collectors.toList());
        return getAliasedComputation(processedQuery, expressions);
    }

    // Remove Top K fields from computations.
    private static Computation extractTopK(ProcessedQuery processedQuery) {
        List<ExpressionNode> expressions = processedQuery.getSelectNodes().stream()
                                                                          .filter(processedQuery::isNotSimpleFieldExpression)
                                                                          .filter(processedQuery::isNotAggregate)
                                                                          .distinct()
                                                                          .collect(Collectors.toList());
        expressions.removeAll(processedQuery.getTopK().getExpressions());
        return getAliasedComputation(processedQuery, expressions);
    }

    // For Special K, the computations are the select fields minus the group by and aggregate fields.
    private static Computation extractSpecialK(ProcessedQuery processedQuery) {
        Set<ExpressionNode> expressions = new HashSet<>(processedQuery.getSelectNodes());
        expressions.removeAll(processedQuery.getGroupByNodes());
        expressions.removeAll(processedQuery.getGroupOpNodes());
        return getAliasedComputation(processedQuery, expressions);
    }

    private static Computation getAliasedComputation(ProcessedQuery processedQuery, Collection<ExpressionNode> expressions) {
        if (expressions.isEmpty()) {
            return null;
        }
        return new Computation(getAliasedFields(processedQuery, expressions));
    }

    private static List<Field> getAliasedFields(ProcessedQuery processedQuery, Collection<ExpressionNode> expressions) {
        ExpressionProcessor.visit(expressions, processedQuery.getPostAggregationMapping());
        processedQuery.setComputation(expressions);
        return expressions.stream().map(toAliasedField(processedQuery)).collect(Collectors.toCollection(ArrayList::new));
    }

    private static Function<ExpressionNode, Field> toAliasedField(ProcessedQuery processedQuery) {
        return node -> new Field(processedQuery.getAliasOrName(node), processedQuery.getPostAggregationMapping().get(node));
    }
}
