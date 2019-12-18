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
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.parsing.Projection;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ProjectionExtractor {
    private ProcessedQuery processedQuery;

    public Projection extractProjection(ProcessedQuery processedQuery) {
        this.processedQuery = processedQuery;
        switch (processedQuery.getQueryType()) {
            case SELECT:
                return extractSelect();
            case SELECT_ALL:
                return null;
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
        List<Projection.Field> fields = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                                                .map(this::toAliasedField)
                                                                                .collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractDistinct() {
        List<Projection.Field> fields = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                                                .map(this::toNonAliasedField)
                                                                                .collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractGroup() {
        // project group by fields and aggregate inner expressions
        Set<ExpressionNode> requiredNodes = new HashSet<>(processedQuery.getGroupByNodes());
        requiredNodes.addAll(processedQuery.getGroupOpNodes().stream().map(GroupOperationNode::getExpression).collect(Collectors.toSet()));
        List<Projection.Field> fields = requiredNodes.stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractCountDistinct() {
        // project count distinct inner expressions
        CountDistinctNode countDistinct = processedQuery.getCountDistinct();
        List<Projection.Field> fields = countDistinct.getExpressions().stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractDistribution() {
        // project distribution inner expression
        DistributionNode distribution = processedQuery.getDistribution();
        List<Projection.Field> fields = Collections.singletonList(toNonAliasedField(distribution.getExpression()));
        return new Projection(fields);
    }

    private Projection extractTopK() {
        // project topk inner expressions
        TopKNode topK = processedQuery.getTopK();
        List<Projection.Field> fields = topK.getExpressions().stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection extractSpecialK() {
        // project group by fields
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
