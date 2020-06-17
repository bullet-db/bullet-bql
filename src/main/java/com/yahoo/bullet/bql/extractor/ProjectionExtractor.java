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
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;

import java.util.ArrayList;
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
        return new Projection(getAliasedFields(processedQuery, expressions), false);
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
        if (expressions.isEmpty()) {
            return new Projection();
        }
        return new Projection(getAliasedFields(processedQuery, expressions), true);
    }

    // Project the select fields without their aliases since they'll be renamed in the aggregation instead.
    private static Projection extractDistinct(ProcessedQuery processedQuery) {
        List<ExpressionNode> expressions = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toList());
        /*
        If all select fields are simple field expressions, then there's no need to project anything.
        Order by computations are done afterward and won't clobber the original record since the aggregation creates a new record.
        */
        return getNonAliasedProjection(processedQuery, expressions);
    }

    /*
    Project group by fields and any fields with aggregates (without their aliases since they'll be renamed in
    the aggregation). We don't care about select or order by fields, since those are computed afterwards.
    */
    private static Projection extractGroup(ProcessedQuery processedQuery) {
        // Project group by and aggregate fields (need to filter for non-null because COUNT(*) does not have a field)
        List<ExpressionNode> expressions =
                Stream.concat(processedQuery.getGroupByNodes().stream(),
                              processedQuery.getGroupOpNodes().stream().map(GroupOperationNode::getExpression).filter(Objects::nonNull))
                      .distinct()
                      .collect(Collectors.toList());
        // If the group by and aggregate fields are all simple field expressions, then we don't need to project anything
        return getNonAliasedProjection(processedQuery, expressions);
    }

    /*
    Project count distinct fields. Ignore any possible aliasing, e.g. SELECT abc AS def, COUNT(DISTINCT abc) FROM STREAM();
    since the other select fields are computed afterward. If the to-be-projected fields are all simple field expressions,
    then there's no need to project anything.
    */
    private static Projection extractCountDistinct(ProcessedQuery processedQuery) {
        return getNonAliasedProjection(processedQuery, processedQuery.getCountDistinct().getExpressions());
    }

    // Same as count distinct.
    private static Projection extractDistribution(ProcessedQuery processedQuery) {
        return getNonAliasedProjection(processedQuery, Collections.singletonList(processedQuery.getDistribution().getExpression()));
    }

    // Same as count distinct and distribution.
    private static Projection extractTopK(ProcessedQuery processedQuery) {
        return getNonAliasedProjection(processedQuery, processedQuery.getTopK().getExpressions());
    }

    // Same as count distinct, distribution, and top k. Ignoring aggregate since guaranteed to have only COUNT(*).
    private static Projection extractSpecialK(ProcessedQuery processedQuery) {
        return getNonAliasedProjection(processedQuery, processedQuery.getGroupByNodes());
    }

    /*
    If the expressions are all simple fields, there's nothing to project and any bullet record can be passed through
    as is.
     */
    private static Projection getNonAliasedProjection(ProcessedQuery processedQuery, List<ExpressionNode> expressions) {
        if (areAllSimpleFields(processedQuery, expressions)) {
            return new Projection();
        }
        return new Projection(getNonAliasedFields(processedQuery, expressions), false);
    }

    private static List<Field> getAliasedFields(ProcessedQuery processedQuery, List<ExpressionNode> expressions) {
        ExpressionProcessor.visit(expressions, processedQuery.getPreAggregationMapping());
        processedQuery.setProjection(expressions);
        return expressions.stream().map(toAliasedField(processedQuery)).collect(Collectors.toCollection(ArrayList::new));
    }

    private static List<Field> getNonAliasedFields(ProcessedQuery processedQuery, List<ExpressionNode> expressions) {
        ExpressionProcessor.visit(expressions, processedQuery.getPreAggregationMapping());
        processedQuery.setProjection(expressions);
        return expressions.stream().map(toNonAliasedField(processedQuery)).collect(Collectors.toCollection(ArrayList::new));
    }
    
    private static boolean areAllSimpleFields(ProcessedQuery processedQuery, Collection<ExpressionNode> nodes) {
        return nodes.stream().allMatch(processedQuery::isSimpleFieldExpression);
    }

    private static Function<ExpressionNode, Field> toAliasedField(ProcessedQuery processedQuery) {
        return node -> new Field(processedQuery.getAliasOrName(node), processedQuery.getPreAggregationMapping().get(node));
    }

    private static Function<ExpressionNode, Field> toNonAliasedField(ProcessedQuery processedQuery) {
        return node -> new Field(node.getName(), processedQuery.getPreAggregationMapping().get(node));
    }
}
