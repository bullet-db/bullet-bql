/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Field;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ComputationExtractor {
    static Computation extractComputation(ProcessedQuery processedQuery) {
        switch (processedQuery.getQueryType()) {
            case SELECT:
                // No computations since everything is handled in the initial projection.
                return null;
            case SELECT_ALL:
                return extractAll(processedQuery);
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

    // Mirrors the projection logic. If the record is passed through, there should be no computations.
    private static Computation extractAll(ProcessedQuery processedQuery) {
        List<Field> fields =
                Stream.concat(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
                      .filter(expression -> processedQuery.isNotSimpleFieldExpression(expression) || processedQuery.hasAlias(expression))
                      .distinct()
                      .map(toAliasedField(processedQuery))
                      .collect(Collectors.toList());
        return new Computation(fields);
    }

    /*
    For SELECT DISTINCT, we only need to consider the computations in ORDER BY. Ignore any clauses that are just
    simple field expressions or a repeat of a select field.
    */
    private static Computation extractDistinct(ProcessedQuery processedQuery) {
        Set<ExpressionNode> orderByFields =
                processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                         .filter(processedQuery::isNotSimpleFieldExpression)
                                                         .collect(Collectors.toSet());
        orderByFields.removeAll(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                                        .collect(Collectors.toList()));
        List<Field> fields = orderByFields.stream().map(toAliasedField(processedQuery)).collect(Collectors.toList());
        return new Computation(fields);
    }

    private static Computation extractAggregate(ProcessedQuery processedQuery) {
        List<Field> fields =
                Stream.concat(processedQuery.getNonAggregateSelectNodes().stream().map(SelectItemNode::getExpression),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
                      .filter(processedQuery::isNotGroupByNode)
                      .filter(processedQuery::isNotSimpleFieldExpression)
                      .distinct()
                      .map(toAliasedField(processedQuery))
                      .collect(Collectors.toList());
        return new Computation(fields);
    }

    /*
    For Special K, the computations are what's left over from the select fields after removing all the group by and
    aggregate fields.
    */
    private static Computation extractSpecialK(ProcessedQuery processedQuery) {
        Set<ExpressionNode> computationNodes = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toSet());
        computationNodes.removeAll(processedQuery.getGroupByNodes());
        computationNodes.removeAll(processedQuery.getGroupOpNodes());
        List<Field> fields = computationNodes.stream().map(toAliasedField(processedQuery)).collect(Collectors.toList());
        return new Computation(fields);
    }

    private static Function<ExpressionNode, Field> toAliasedField(ProcessedQuery processedQuery) {
        return node -> new Field(processedQuery.getAliasOrName(node), processedQuery.getExpression(node));
    }
}
