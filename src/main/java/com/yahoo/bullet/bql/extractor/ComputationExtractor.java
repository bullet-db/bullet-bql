/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.classifier.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Projection;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ComputationExtractor {
    private ProcessedQuery processedQuery;

    public Computation extractComputation(ProcessedQuery processedQuery) {
        this.processedQuery = processedQuery;
        switch (processedQuery.getQueryType()) {
            case SELECT:
                return null;
            case SELECT_ALL:
                return extractAll();
            case SELECT_DISTINCT:
                return null;
            case GROUP:
            case COUNT_DISTINCT:
            case DISTRIBUTION:
            case TOP_K:
                return extractAggregate();
            //case GROUP:
            //    return extractGroup();
            //case COUNT_DISTINCT:
            //    return extractCountDistinct();
            //case DISTRIBUTION:
            //    return extractDistribution();
            //case TOP_K:
            //    return extractTopK();
            case SPECIAL_K:
                return extractSpecialK();
        }
        throw new ParsingException("Unsupported");
    }

    private Computation extractAll() {
        List<Projection.Field> fields = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                                                .map(this::toField)
                                                                                .collect(Collectors.toList());
        return new Computation(new Projection(fields));
    }

    private Computation extractAggregate() {
        List<Projection.Field> fields = processedQuery.getNonAggregateSelectNodes().stream().map(SelectItemNode::getExpression)
                                                                                            .map(this::toField)
                                                                                            .collect(Collectors.toList());
        return new Computation(new Projection(fields));
    }

    private Computation extractGroup() {
        List<Projection.Field> fields = processedQuery.getSuperAggregateSelectNodes().stream().map(SelectItemNode::getExpression)
                                                                                              .map(this::toField)
                                                                                              .collect(Collectors.toList());
        return new Computation(new Projection(fields));
    }

    private Computation extractCountDistinct() {
        List<Projection.Field> fields = processedQuery.getSuperAggregateSelectNodes().stream().map(SelectItemNode::getExpression)
                                                                                              .map(this::toField)
                                                                                              .collect(Collectors.toList());
        return new Computation(new Projection(fields));
    }

    private Computation extractDistribution() {
        List<Projection.Field> fields = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                                                .filter(Objects::nonNull)
                                                                                .map(this::toField)
                                                                                .collect(Collectors.toList());
        return new Computation(new Projection(fields));
    }

    private Computation extractTopK() {
        List<Projection.Field> fields = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                                                .filter(Objects::nonNull)
                                                                                .map(this::toField)
                                                                                .collect(Collectors.toList());
        return new Computation(new Projection(fields));
    }

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
