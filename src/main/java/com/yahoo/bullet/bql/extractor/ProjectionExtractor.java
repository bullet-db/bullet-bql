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
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression).filter(processedQuery::isNotAliasFieldExpression))
                        .distinct()
                        .map(this::toAliasedField)
                        .collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractSelectAll() {
        if (processedQuery.getSelectNodes().isEmpty()) {
            return null;
        }
        // Proje
        if (processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }
        // Perform a copy when there are additional fields selected
        return new Projection();
    }

    private Projection extractDistinct() {
        // Project DISTINCT expressions UNLESS all expressions are simple field expressions
        List<ExpressionNode> expressions = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toList());
        if (expressions.stream().allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }
        List<Projection.Field> fields = expressions.stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractGroup() {
        // Project GROUP BY fields
        Set<ExpressionNode> requiredNodes = new HashSet<>(processedQuery.getGroupByNodes());

        // Project aggregate inner expressions (need to filter for non-null because COUNT(*) does not have an inner expression)
        requiredNodes.addAll(processedQuery.getGroupOpNodes().stream().map(GroupOperationNode::getExpression).filter(Objects::nonNull).collect(Collectors.toSet()));

        // If inner expressions are all simple field expressions, we don't need to project anything
        if (requiredNodes.stream().allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }
        List<Projection.Field> fields = requiredNodes.stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractCountDistinct() {
        // Project COUNT DISTINCT inner expressions UNLESS all inner expressions are simple field expressions
        CountDistinctNode countDistinct = processedQuery.getCountDistinct();
        if (countDistinct.getExpressions().stream().allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }
        List<Projection.Field> fields = countDistinct.getExpressions().stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractDistribution() {
        // Project DISTRIBUTION inner expression unless it's a simple field expression
        DistributionNode distribution = processedQuery.getDistribution();
        if (processedQuery.isSimpleFieldExpression(distribution.getExpression())) {
            return null;
        }
        List<Projection.Field> fields = Collections.singletonList(toNonAliasedField(distribution.getExpression()));
        return new Projection(fields);
    }

    private Projection extractTopK() {
        // Project Top K inner expressions
        TopKNode topK = processedQuery.getTopK();
        if (topK.getExpressions().stream().allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }
        List<Projection.Field> fields = topK.getExpressions().stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractSpecialK() {
        // Project GROUP BY fields
        if (processedQuery.getGroupByNodes().stream().allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }
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
