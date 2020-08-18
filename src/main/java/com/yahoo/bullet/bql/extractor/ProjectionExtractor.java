/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ExpressionProcessor;
import com.yahoo.bullet.bql.query.ComputableProcessor;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.query.OrderByProcessor;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    // Projects all SELECT fields and any simple fields ORDER BY is dependent on.
    private static Projection extractSelect(ProcessedQuery processedQuery) {
        // The set of fields that will be in the schema from SELECT
        processedQuery.setSelectNames(Stream.concat(processedQuery.getSelectNodes().stream().filter(processedQuery::isSimpleFieldExpression).map(ExpressionNode::getName),
                                                    processedQuery.getAliases().values().stream())
                                            .collect(Collectors.toSet()));

        // Add incomputable fields to the postaggregation mapping
        Map<ExpressionNode, Expression> mapping = processedQuery.getPostAggregationMapping();
        ComputableProcessor.visit(processedQuery.getSelectNodes(), processedQuery)
                           .forEach(node -> mapping.put(node, new FieldExpression(processedQuery.getAliasOrName(node))));

        // Populates a set of fields that ORDER BY clauses depend on but are not in SELECT and not in the postaggregation mapping
        OrderByProcessor.visit(processedQuery.getOrderByNodes(), processedQuery);

        List<ExpressionNode> expressions = Stream.concat(processedQuery.getSelectNodes().stream(),
                                                         processedQuery.getOrderByExtraSelectNodes().stream())
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
                processedQuery.getSelectNodes().stream().filter(node -> processedQuery.isNotSimpleFieldExpression(node) ||
                                                                        processedQuery.hasAlias(node))
                                                        .collect(Collectors.toList());
        if (expressions.isEmpty()) {
            return new Projection();
        }
        return new Projection(getAliasedFields(processedQuery, expressions), true);
    }

    // Project the select fields without their aliases since they'll be renamed in the aggregation instead.
    private static Projection extractDistinct(ProcessedQuery processedQuery) {
        // If all select fields are simple field expressions, then there's no need to project anything.
        return getNonAliasedProjection(processedQuery, processedQuery.getSelectNodes());
    }

    /*
    Project group by fields and any fields with aggregates (without their aliases since they'll be renamed in
    the aggregation). We don't care about select fields, since those are computed afterwards.
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
    private static Projection getNonAliasedProjection(ProcessedQuery processedQuery, Collection<ExpressionNode> expressions) {
        if (areAllSimpleFields(processedQuery, expressions)) {
            return new Projection();
        }
        return new Projection(getNonAliasedFields(processedQuery, expressions), false);
    }

    private static List<Field> getAliasedFields(ProcessedQuery processedQuery, Collection<ExpressionNode> expressions) {
        ExpressionProcessor.visit(expressions, processedQuery.getPreAggregationMapping());
        processedQuery.setProjection(expressions);
        return expressions.stream().map(toAliasedField(processedQuery)).collect(Collectors.toCollection(ArrayList::new));
    }

    private static List<Field> getNonAliasedFields(ProcessedQuery processedQuery, Collection<ExpressionNode> expressions) {
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
