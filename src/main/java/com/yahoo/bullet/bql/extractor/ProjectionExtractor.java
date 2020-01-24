/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.processor.ProcessedQuery;
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
                return extractAll();
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
        throw new ParsingException("Unreachable");
    }

    /**
     * Need to project all SELECT fields and any ORDER BY fields that are not simple alias fields. For example, if we
     * select a field "abc" AS "def" and order by "def", we don't want to end up projecting "def" along with "abc" since
     * the order by was actually referring to "abc". If "abc" wasn't aliased as "def", we would want to project "def"
     * for order by though. So, anything else that shows up in ORDER BY needs to be projected.
     */
    private Projection extractSelect() {
        List<Projection.Field> fields =
                Stream.concat(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression).filter(processedQuery::isNotSimpleAliasFieldExpression))
                        .distinct()
                        .map(this::toAliasedField)
                        .collect(Collectors.toList());
        return new Projection(fields);
    }

    /**
     * For SELECT *, we decide whether or not the query will have computations. If the query doesn't have any computations,
     * we can just pass the record through. If there are computations, we return a projection with null fields which
     * will copy the record instead.
     */
    private Projection extractAll() {
        List<ExpressionNode> expressions =
                Stream.concat(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
                        .collect(Collectors.toList());
        // If the only additional fields selected (including ORDER BY) are simple field expressions and none of
        // them have aliases, then no additional computations will be made
        if (expressions.stream().allMatch(processedQuery::isSimpleFieldExpression) &&
            expressions.stream().noneMatch(processedQuery::hasAlias)) {
            return null;
        }
        // Perform a copy when there are additional fields selected
        return new Projection();
    }

    /**
     * Project the select fields without their aliases since they'll be renamed in the aggregation instead.
     */
    private Projection extractDistinct() {
        List<ExpressionNode> expressions = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toList());
        // If all select fields are simple field expressions, then there's no need to project anything.
        // Order by computations are done afterward and won't clobber the original record since the aggregation creates a new record.
        if (expressions.stream().allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }
        List<Projection.Field> fields = expressions.stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    /**
     * Project group by fields and any fields with aggregates (without their aliases since they'll be renamed in
     * the aggregation). We care about select or order by fields, since those are computed afterwards.
     */
    private Projection extractGroup() {
        Set<ExpressionNode> requiredNodes = new HashSet<>(processedQuery.getGroupByNodes());

        // Project aggregate fields (need to filter for non-null because COUNT(*) does not have a field)
        requiredNodes.addAll(processedQuery.getGroupOpNodes().stream().map(GroupOperationNode::getExpression)
                                                                      .filter(Objects::nonNull)
                                                                      .collect(Collectors.toSet()));

        // If the group by and aggregate fields are all simple field expressions, then we don't need to project anything
        if (requiredNodes.stream().allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }
        List<Projection.Field> fields = requiredNodes.stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    /**
     * Project count distinct fields. Ignore any possible aliasing, e.g. SELECT abc AS def, COUNT(DISTINCT abc) FROM STREAM();
     * since the other select fields are computed afterward. If the to-be-projected fields are all simple field expressions,
     * then there's no need to project anything.
     */
    private Projection extractCountDistinct() {
        CountDistinctNode countDistinct = processedQuery.getCountDistinct();
        if (countDistinct.getExpressions().stream().allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }
        List<Projection.Field> fields = countDistinct.getExpressions().stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    /**
     * Same as count distinct.
     */
    private Projection extractDistribution() {
        DistributionNode distribution = processedQuery.getDistribution();
        if (processedQuery.isSimpleFieldExpression(distribution.getExpression())) {
            return null;
        }
        List<Projection.Field> fields = Collections.singletonList(toNonAliasedField(distribution.getExpression()));
        return new Projection(fields);
    }

    /**
     * Same as count distinct and distribution.
     */
    private Projection extractTopK() {
        TopKNode topK = processedQuery.getTopK();
        if (topK.getExpressions().stream().allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }
        List<Projection.Field> fields = topK.getExpressions().stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    /**
     * Same as count distinct, distribution, and top k. Ignoring aggregate since guaranteed to have only COUNT(*).
     */
    private Projection extractSpecialK() {
        if (processedQuery.getGroupByNodes().stream().allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }
        List<Projection.Field> fields = processedQuery.getGroupByNodes().stream().map(this::toNonAliasedField).collect(Collectors.toList());
        return new Projection(fields);
    }

    private Projection.Field toAliasedField(ExpressionNode node) {
        return new Projection.Field(processedQuery.getAliasOrName(node), processedQuery.getExpression(node));
    }

    private Projection.Field toNonAliasedField(ExpressionNode node) {
        return new Projection.Field(node.getName(), processedQuery.getExpression(node));
    }
}
