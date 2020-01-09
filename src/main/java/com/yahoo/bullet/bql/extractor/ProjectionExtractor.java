/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.classifier.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.parsing.Projection;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProjectionExtractor {
    private ProcessedQuery processedQuery;

    public Projection extractProjection(ProcessedQuery processedQuery) {
        this.processedQuery = processedQuery;
        switch (processedQuery.getQueryType()) {
            case SELECT:
                return extractSelect();
            case SELECT_ALL:
                return extractSelectAll();
            case SELECT_DISTINCT:
                return extractDistinct();
            case GROUP:
                return extractGroup();
            case COUNT_DISTINCT:
                return extractCountDistinct();
            case DISTRIBUTION:
                return extractDistribution();
            case TOP_K:
                return extractTopK();
            case SPECIAL_K:
                return extractSpecialK();
        }
        throw new ParsingException("Unsupported");
    }

    private Projection extractSelect() {
        List<Projection.Field> fields =
                Stream.concat(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
                        .filter(processedQuery::isNotAliasReference)
                        .distinct()
                        .map(this::toAliasedField)
                        .collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractSelectAll() {
        if (processedQuery.getSelectNodes().isEmpty()) {
            return null;
        }
        // Perform a copy when there are additional fields selected
        return new Projection();
    }

    private Projection extractDistinct() {
        List<Projection.Field> fields = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                                                .map(this::toNonAliasedField)
                                                                                .collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractGroup() {
        // Project GROUP BY fields
        Set<ExpressionNode> requiredNodes = new HashSet<>(processedQuery.getGroupByNodes());

        // Project aggregate inner expressions (need to filter for non-null because COUNT(*) does not have an inner expression)
        requiredNodes.addAll(processedQuery.getGroupOpNodes().stream().map(GroupOperationNode::getExpression).filter(Objects::nonNull).collect(Collectors.toSet()));
        List<Projection.Field> fields = requiredNodes.stream().map(this::toNonAliasedField).collect(Collectors.toList());

        // Can be empty if SELECT COUNT(*) FROM STREAM();
        if (fields.isEmpty()) {
            return null;
        }

        return new Projection(fields);
    }

    private Projection extractCountDistinct() {
        // Project COUNT DISTINCT inner expressions
        CountDistinctNode countDistinct = processedQuery.getCountDistinct();
        List<Projection.Field> fields = countDistinct.getExpressions().stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractDistribution() {
        // Project DISTRIBUTION inner expression
        DistributionNode distribution = processedQuery.getDistribution();
        List<Projection.Field> fields = Collections.singletonList(toNonAliasedField(distribution.getExpression()));
        return new Projection(fields);
    }

    private Projection extractTopK() {
        // Project Top K inner expressions
        TopKNode topK = processedQuery.getTopK();
        List<Projection.Field> fields = topK.getExpressions().stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractSpecialK() {
        // Project GROUP BY fields
        List<Projection.Field> fields = processedQuery.getGroupByNodes().stream().map(this::toAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection.Field toAliasedField(ExpressionNode node) {
        return new Projection.Field(processedQuery.getAliasOrName(node), processedQuery.getExpression(node));
    }

    private Projection.Field toNonAliasedField(ExpressionNode node) {
        return new Projection.Field(node.toFormatlessString(), processedQuery.getExpression(node));
    }
}
