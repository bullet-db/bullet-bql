/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.parsing.Field;
import com.yahoo.bullet.parsing.Projection;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProjectionExtractor {
    static Projection extractProjection(ProcessedQuery processedQuery) {
        switch (processedQuery.getQueryType()) {
            case SELECT:
                return extractSelect(processedQuery);
            case SELECT_ALL:
                return extractAll(processedQuery);
            case SELECT_DISTINCT:
                return extractDistinct(processedQuery);
            case GROUP:
                return extractGroup(processedQuery);
            case COUNT_DISTINCT:
                return extractCountDistinct(processedQuery);
            case DISTRIBUTION:
                return extractDistribution(processedQuery);
            case TOP_K:
                return extractTopK(processedQuery);
            case SPECIAL_K:
                return extractSpecialK(processedQuery);
        }
        throw new ParsingException("Unknown query type");
    }

    /*
    Need to project all SELECT fields and any ORDER BY fields that are not simple alias fields. For example, if we
    select a field "abc" AS "def" and order by "def", we don't want to end up projecting "def" along with "abc" since
    the order by was actually referring to "abc". If "abc" wasn't aliased as "def", we would want to project "def"
    for order by though. So, anything else that shows up in ORDER BY needs to be projected.
    */
    private static Projection extractSelect(ProcessedQuery processedQuery) {
        List<ExpressionNode> expressions =
                Stream.concat(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression).filter(processedQuery::isNotSimpleAliasFieldExpression))
                      .distinct()
                      .collect(Collectors.toList());
        processedQuery.setProjectionNodes(expressions);
        List<Field> fields = expressions.stream().map(toAliasedField(processedQuery)).collect(Collectors.toList());
        return new Projection(fields);
    }

    /*
    For SELECT *, we decide whether or not the query will have computations. If the query doesn't have any computations,
    we can just pass the record through. If there are computations, we return a projection with null fields which
    will copy the record instead.
    */
    private static Projection extractAll(ProcessedQuery processedQuery) {
        List<ExpressionNode> expressions =
                Stream.concat(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
                      .filter(node -> processedQuery.isNotSimpleFieldExpression(node) || processedQuery.hasAlias(node))
                      .distinct()
                      .collect(Collectors.toList());
        /*
        If the only additional fields selected (including ORDER BY) are simple field expressions and none of
        them have aliases, then no additional computations will be made.
        */
        //if (areAllSimpleNonAliasedFields(processedQuery, expressions)) {
        if (expressions.isEmpty()) {
            return null;
        }
        processedQuery.setProjectionNodes(expressions);
        List<Field> fields = expressions.stream().map(toAliasedField(processedQuery)).collect(Collectors.toList());
        // Perform a copy when there are additional fields selected
        return new Projection(fields, true);
    }

    // Project the select fields without their aliases since they'll be renamed in the aggregation instead.
    private static Projection extractDistinct(ProcessedQuery processedQuery) {
        List<ExpressionNode> expressions = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toList());
        /*
        If all select fields are simple field expressions, then there's no need to project anything.
        Order by computations are done afterward and won't clobber the original record since the aggregation creates a new record.
        */
        if (expressions.stream().allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }
        processedQuery.setProjectionNodes(expressions);
        List<Field> fields = expressions.stream().map(toNonAliasedField(processedQuery)).collect(Collectors.toList());
        return new Projection(fields);
    }

    /*
    Project group by fields and any fields with aggregates (without their aliases since they'll be renamed in
    the aggregation). We care about select or order by fields, since those are computed afterwards.
    */
    private static Projection extractGroup(ProcessedQuery processedQuery) {
        // Project group by and aggregate fields (need to filter for non-null because COUNT(*) does not have a field)
        List<ExpressionNode> expressions =
                Stream.concat(processedQuery.getGroupByNodes().stream(),
                              processedQuery.getGroupOpNodes().stream().map(GroupOperationNode::getExpression).filter(Objects::nonNull))
                      .distinct()
                      .collect(Collectors.toList());

        // If the group by and aggregate fields are all simple field expressions, then we don't need to project anything
        if (areAllSimpleFields(processedQuery, expressions)) {
            return null;
        }
        processedQuery.setProjectionNodes(expressions);
        List<Field> fields = expressions.stream().map(toNonAliasedField(processedQuery)).collect(Collectors.toList());
        return new Projection(fields);
    }

    /*
    Project count distinct fields. Ignore any possible aliasing, e.g. SELECT abc AS def, COUNT(DISTINCT abc) FROM STREAM();
    since the other select fields are computed afterward. If the to-be-projected fields are all simple field expressions,
    then there's no need to project anything.
    */
    private static Projection extractCountDistinct(ProcessedQuery processedQuery) {
        CountDistinctNode countDistinct = processedQuery.getCountDistinct();
        if (areAllSimpleFields(processedQuery, countDistinct.getExpressions())) {
            return null;
        }
        processedQuery.setProjectionNodes(countDistinct.getExpressions());
        List<Field> fields = countDistinct.getExpressions().stream().map(toNonAliasedField(processedQuery)).collect(Collectors.toList());
        return new Projection(fields);
    }

    // Same as count distinct.
    private static Projection extractDistribution(ProcessedQuery processedQuery) {
        DistributionNode distribution = processedQuery.getDistribution();
        if (processedQuery.isSimpleFieldExpression(distribution.getExpression())) {
            return null;
        }
        processedQuery.setProjectionNodes(Collections.singletonList(distribution.getExpression()));
        List<Field> fields = Collections.singletonList(toNonAliasedField(processedQuery, distribution.getExpression()));
        return new Projection(fields);
    }

    // Same as count distinct and distribution.
    private static Projection extractTopK(ProcessedQuery processedQuery) {
        TopKNode topK = processedQuery.getTopK();
        if (areAllSimpleFields(processedQuery, topK.getExpressions())) {
            return null;
        }
        processedQuery.setProjectionNodes(topK.getExpressions());
        List<Field> fields = topK.getExpressions().stream().map(toNonAliasedField(processedQuery)).collect(Collectors.toList());
        return new Projection(fields);
    }

    // Same as count distinct, distribution, and top k. Ignoring aggregate since guaranteed to have only COUNT(*).
    private static Projection extractSpecialK(ProcessedQuery processedQuery) {
        if (areAllSimpleFields(processedQuery, processedQuery.getGroupByNodes())) {
            return null;
        }
        processedQuery.setProjectionNodes(processedQuery.getGroupByNodes());
        List<Field> fields = processedQuery.getGroupByNodes().stream().map(toNonAliasedField(processedQuery)).collect(Collectors.toList());
        return new Projection(fields);
    }
    
    private static boolean areAllSimpleFields(ProcessedQuery processedQuery, Collection<ExpressionNode> nodes) {
        return nodes.stream().allMatch(processedQuery::isSimpleFieldExpression);
    }

    private static boolean areAllSimpleNonAliasedFields(ProcessedQuery processedQuery, Collection<ExpressionNode> nodes) {
        return nodes.stream().allMatch(processedQuery::isSimpleFieldExpression) &&
               nodes.stream().noneMatch(processedQuery::hasAlias);
    }

    private static Function<ExpressionNode, Field> toAliasedField(ProcessedQuery processedQuery) {
        return node -> new Field(processedQuery.getAliasOrName(node), processedQuery.getExpression(node));
    }

    private static Field toNonAliasedField(ProcessedQuery processedQuery, ExpressionNode node) {
        return new Field(node.getName(), processedQuery.getExpression(node));
    }

    private static Function<ExpressionNode, Field> toNonAliasedField(ProcessedQuery processedQuery) {
        return node -> new Field(node.getName(), processedQuery.getExpression(node));
    }
}
