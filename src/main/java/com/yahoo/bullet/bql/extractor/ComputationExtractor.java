/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.processor.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Projection;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ComputationExtractor {
    private ProcessedQuery processedQuery;

    public Computation extractComputation(ProcessedQuery processedQuery) {
        this.processedQuery = processedQuery;
        switch (processedQuery.getQueryType()) {
            case SELECT:
                // No computations since everything is handled in the initial projection.
                return null;
            case SELECT_ALL:
                return extractAll();
            case SELECT_DISTINCT:
                return extractDistinct();
            case GROUP:
            case COUNT_DISTINCT:
            case DISTRIBUTION:
            case TOP_K:
                return extractAggregate();
            case SPECIAL_K:
                return extractSpecialK();
        }
        throw new ParsingException("Unreachable");
    }

    /**
     * Mirrors the projection logic. If the record is passed through, there should be no computations.
     */
    private Computation extractAll() {
        List<Projection.Field> fields =
                Stream.concat(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
                        .filter(expression -> processedQuery.isNotSimpleFieldExpression(expression) || processedQuery.hasAlias(expression))
                        .distinct()
                        .map(this::toField)
                        .collect(Collectors.toList());
        return new Computation(new Projection(fields));
    }

    /**
     * For SELECT DISTINCT, we only need to consider the computations in ORDER BY. Ignore any clauses that are just
     * simple field expressions or a repeat of a select field.
     */
    private Computation extractDistinct() {
        Set<ExpressionNode> orderByFields =
                processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                         .filter(processedQuery::isNotSimpleFieldExpression)
                                                         .collect(Collectors.toSet());
        orderByFields.removeAll(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                                        .collect(Collectors.toList()));
        List<Projection.Field> fields = orderByFields.stream().map(this::toField).collect(Collectors.toList());
        return new Computation(new Projection(fields));
    }

    /**
     *
     */
    private Computation extractAggregate() {
        List<Projection.Field> fields =
                Stream.concat(processedQuery.getNonAggregateSelectNodes().stream().map(SelectItemNode::getExpression),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
                        .filter(processedQuery::isNotGroupByNode)
                        .filter(processedQuery::isNotSimpleFieldExpression)
                        .distinct()
                        .map(this::toField)
                        .collect(Collectors.toList());
        return new Computation(new Projection(fields));
    }

    /**
     * For Special K, the computations are what's left over from the select fields after removing all the group by and
     * aggregate fields.
     */
    private Computation extractSpecialK() {
        Set<ExpressionNode> computationNodes = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toSet());
        computationNodes.removeAll(processedQuery.getGroupByNodes());
        computationNodes.removeAll(processedQuery.getGroupOpNodes());
        List<Projection.Field> fields = computationNodes.stream().map(this::toField).collect(Collectors.toList());
        return new Computation(new Projection(fields));
    }

    private Projection.Field toField(ExpressionNode node) {
        return new Projection.Field(processedQuery.getAliasOrName(node), processedQuery.getExpression(node));
    }
}
