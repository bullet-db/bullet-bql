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
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Field;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ComputationExtractor {
    static Computation extractComputation(ProcessedQuery processedQuery) {
        switch (processedQuery.getQueryType()) {
            case SELECT:
            case SELECT_ALL:
                // No computations since everything is handled in the initial projection.
                return null;
            case SELECT_DISTINCT:
                return extractDistinct(processedQuery);
            case GROUP:
            case COUNT_DISTINCT:
            case DISTRIBUTION:
            case TOP_K:
                return extractAggregate(processedQuery);
            case SPECIAL_K:
                return extractSpecialK(processedQuery);
        }
        throw new ParsingException("Unknown query type");
    }

    /*
    For SELECT DISTINCT, we only need to consider the computations in ORDER BY. Ignore any clauses that are just
    simple field expressions or a repeat of a select field.
    */
    private static Computation extractDistinct(ProcessedQuery processedQuery) {
        Set<ExpressionNode> expressions = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                                   .filter(processedQuery::isNotSimpleFieldExpression)
                                                                                   .collect(Collectors.toSet());
        processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).forEach(expressions::remove);
        return new Computation(getAliasedFields(processedQuery, expressions));
    }

    private static Computation extractAggregate(ProcessedQuery processedQuery) {
        List<ExpressionNode> expressions =
                Stream.concat(processedQuery.getNonAggregateSelectNodes().stream().map(SelectItemNode::getExpression),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
                      .filter(processedQuery::isNotGroupByNode)
                      .filter(processedQuery::isNotSimpleFieldExpression)
                      .filter(processedQuery::isNotAggregate)
                      .distinct()
                      .collect(Collectors.toList());
        return new Computation(getAliasedFields(processedQuery, expressions));
    }

    /*
    For Special K, the computations are what's left over from the select fields after removing all the group by and
    aggregate fields.
    */
    private static Computation extractSpecialK(ProcessedQuery processedQuery) {
        Set<ExpressionNode> expressions = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toSet());
        expressions.removeAll(processedQuery.getGroupByNodes());
        expressions.removeAll(processedQuery.getGroupOpNodes());
        return new Computation(getAliasedFields(processedQuery, expressions));
    }

    private static List<Field> getAliasedFields(ProcessedQuery processedQuery, Collection<ExpressionNode> expressions) {
        ExpressionProcessor.visit(expressions, processedQuery.getPostAggregationMapping());
        processedQuery.setComputation(expressions);
        return expressions.stream().map(toAliasedField(processedQuery)).collect(Collectors.toList());
    }

    private static Function<ExpressionNode, Field> toAliasedField(ProcessedQuery processedQuery) {
        return node -> new Field(processedQuery.getAliasOrName(node), processedQuery.getPostAggregationMapping().get(node));
    }
}
