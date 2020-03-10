/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ExpressionProcessor;
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
import com.yahoo.bullet.parsing.expressions.Expression;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProjectionExtractor {
    static Projection extractProjection(ProcessedQuery processedQuery, ExpressionProcessor expressionProcessor) {
        switch (processedQuery.getQueryType()) {
            case SELECT:
                return extractSelect(processedQuery, expressionProcessor);
            case SELECT_ALL:
                return extractAll(processedQuery, expressionProcessor);
            case SELECT_DISTINCT:
                return extractDistinct(processedQuery, expressionProcessor);
            case GROUP:
                return extractGroup(processedQuery, expressionProcessor);
            case COUNT_DISTINCT:
                return extractCountDistinct(processedQuery, expressionProcessor);
            case DISTRIBUTION:
                return extractDistribution(processedQuery, expressionProcessor);
            case TOP_K:
                return extractTopK(processedQuery, expressionProcessor);
            case SPECIAL_K:
                return extractSpecialK(processedQuery, expressionProcessor);
        }
        throw new ParsingException("Unknown query type");
    }

    /*
    Need to project all SELECT fields and any ORDER BY fields that are not simple alias fields. For example, if we
    select a field "abc" AS "def" and order by "def", we don't want to end up projecting "def" along with "abc" since
    the order by was actually referring to "abc". If "abc" wasn't aliased as "def", we would want to project "def"
    for order by though. So, anything else that shows up in ORDER BY needs to be projected.
    */
    private static Projection extractSelect(ProcessedQuery processedQuery, ExpressionProcessor expressionProcessor) {
        List<ExpressionNode> expressions =
                Stream.concat(processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression).filter(processedQuery::isNotSimpleAliasFieldExpression))
                      .distinct()
                      .collect(Collectors.toList());

        Map<ExpressionNode, Expression> mapping = new HashMap<>();
        expressionProcessor.process(expressions, mapping);

        processedQuery.setProjectionNodes(expressions);
        processedQuery.setProjectionMapping(mapping);

        List<Field> fields = expressions.stream().map(toAliasedField(processedQuery, mapping)).collect(Collectors.toList());
        return new Projection(fields);
    }

    /*
    For SELECT *, we decide whether or not the query will have computations. If the query doesn't have any computations,
    we can just pass the record through. If there are computations, we return a projection with null fields which
    will copy the record instead.
    */
    private static Projection extractAll(ProcessedQuery processedQuery, ExpressionProcessor expressionProcessor) {
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
            return null;
        }

        Map<ExpressionNode, Expression> mapping = new HashMap<>();
        expressionProcessor.process(expressions, mapping);

        processedQuery.setProjectionNodes(expressions);
        processedQuery.setProjectionMapping(mapping);

        List<Field> fields = expressions.stream().map(toAliasedField(processedQuery, mapping)).collect(Collectors.toList());
        // Perform a copy when there are additional fields selected
        return new Projection(fields, true);
    }

    // Project the select fields without their aliases since they'll be renamed in the aggregation instead.
    private static Projection extractDistinct(ProcessedQuery processedQuery, ExpressionProcessor expressionProcessor) {
        List<ExpressionNode> expressions = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toList());
        /*
        If all select fields are simple field expressions, then there's no need to project anything.
        Order by computations are done afterward and won't clobber the original record since the aggregation creates a new record.
        */
        if (expressions.stream().allMatch(processedQuery::isSimpleFieldExpression)) {
            return null;
        }

        Map<ExpressionNode, Expression> mapping = new HashMap<>();
        expressionProcessor.process(expressions, mapping);

        processedQuery.setProjectionNodes(expressions);
        processedQuery.setProjectionMapping(mapping);

        List<Field> fields = expressions.stream().map(toNonAliasedField(mapping)).collect(Collectors.toList());
        return new Projection(fields);
    }

    /*
    Project group by fields and any fields with aggregates (without their aliases since they'll be renamed in
    the aggregation). We care about select or order by fields, since those are computed afterwards.
    */
    private static Projection extractGroup(ProcessedQuery processedQuery, ExpressionProcessor expressionProcessor) {
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

        Map<ExpressionNode, Expression> mapping = new HashMap<>();
        expressionProcessor.process(expressions, mapping);

        processedQuery.setProjectionNodes(expressions);
        processedQuery.setProjectionMapping(mapping);

        List<Field> fields = expressions.stream().map(toNonAliasedField(mapping)).collect(Collectors.toList());
        return new Projection(fields);
    }

    /*
    Project count distinct fields. Ignore any possible aliasing, e.g. SELECT abc AS def, COUNT(DISTINCT abc) FROM STREAM();
    since the other select fields are computed afterward. If the to-be-projected fields are all simple field expressions,
    then there's no need to project anything.
    */
    private static Projection extractCountDistinct(ProcessedQuery processedQuery, ExpressionProcessor expressionProcessor) {
        CountDistinctNode countDistinct = processedQuery.getCountDistinct();
        if (areAllSimpleFields(processedQuery, countDistinct.getExpressions())) {
            return null;
        }

        Map<ExpressionNode, Expression> mapping = new HashMap<>();
        expressionProcessor.process(countDistinct.getExpressions(), mapping);

        processedQuery.setProjectionNodes(countDistinct.getExpressions());
        processedQuery.setProjectionMapping(mapping);

        List<Field> fields = countDistinct.getExpressions().stream().map(toNonAliasedField(mapping)).collect(Collectors.toList());
        return new Projection(fields);
    }

    // Same as count distinct.
    private static Projection extractDistribution(ProcessedQuery processedQuery, ExpressionProcessor expressionProcessor) {
        DistributionNode distribution = processedQuery.getDistribution();
        if (processedQuery.isSimpleFieldExpression(distribution.getExpression())) {
            return null;
        }

        Map<ExpressionNode, Expression> mapping = new HashMap<>();
        expressionProcessor.process(distribution.getExpression(), mapping);

        processedQuery.setProjectionNodes(Collections.singletonList(distribution.getExpression()));
        processedQuery.setProjectionMapping(mapping);

        List<Field> fields = Collections.singletonList(toNonAliasedField(mapping, distribution.getExpression()));
        return new Projection(fields);
    }

    // Same as count distinct and distribution.
    private static Projection extractTopK(ProcessedQuery processedQuery, ExpressionProcessor expressionProcessor) {
        TopKNode topK = processedQuery.getTopK();
        if (areAllSimpleFields(processedQuery, topK.getExpressions())) {
            return null;
        }

        Map<ExpressionNode, Expression> mapping = new HashMap<>();
        expressionProcessor.process(topK.getExpressions(), mapping);

        processedQuery.setProjectionNodes(topK.getExpressions());
        processedQuery.setProjectionMapping(mapping);

        List<Field> fields = topK.getExpressions().stream().map(toNonAliasedField(mapping)).collect(Collectors.toList());
        return new Projection(fields);
    }

    // Same as count distinct, distribution, and top k. Ignoring aggregate since guaranteed to have only COUNT(*).
    private static Projection extractSpecialK(ProcessedQuery processedQuery, ExpressionProcessor expressionProcessor) {
        if (areAllSimpleFields(processedQuery, processedQuery.getGroupByNodes())) {
            return null;
        }

        Map<ExpressionNode, Expression> mapping = new HashMap<>();
        expressionProcessor.process(processedQuery.getGroupByNodes(), mapping);

        processedQuery.setProjectionNodes(processedQuery.getGroupByNodes());
        processedQuery.setProjectionMapping(mapping);

        List<Field> fields = processedQuery.getGroupByNodes().stream().map(toNonAliasedField(mapping)).collect(Collectors.toList());
        return new Projection(fields);
    }
    
    private static boolean areAllSimpleFields(ProcessedQuery processedQuery, Collection<ExpressionNode> nodes) {
        return nodes.stream().allMatch(processedQuery::isSimpleFieldExpression);
    }

    private static Function<ExpressionNode, Field> toAliasedField(ProcessedQuery processedQuery, Map<ExpressionNode, Expression> mapping) {
        return node -> new Field(processedQuery.getAliasOrName(node), mapping.get(node));
    }

    private static Field toNonAliasedField(Map<ExpressionNode, Expression> mapping, ExpressionNode node) {
        return new Field(node.getName(), mapping.get(node));
    }

    private static Function<ExpressionNode, Field> toNonAliasedField(Map<ExpressionNode, Expression> mapping) {
        return node -> new Field(node.getName(), mapping.get(node));
    }
}
