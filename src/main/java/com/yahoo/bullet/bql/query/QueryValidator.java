/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.aggregations.Distribution;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.aggregations.TopK;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.query.postaggregations.PostAggregation;
import com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * RAW PASS_THROUGH queries pass Bullet records as is through projection and aggregation. This means that BQL
 * should not generate post-aggregations such as computation and culling for these queries as these can modify
 * records.
 *
 * A RAW aggregation is only possible for SELECT and SELECT_ALL queries. Out of these two, only SELECT_ALL queries
 * can have a PASS_THROUGH projection. This means any RAW PASS_THROUGH query is also a SELECT_ALL query.
 * SELECT_ALL queries exit early in the culling extractor if they have a PASS_THROUGH projection (i.e. no
 * projection expression nodes in its ProcessedQuery), and SELECT_ALL queries exit early in the computation extractor.
 *
 * In this way, BQL cannot / does not generate computation and culling post-aggregations for RAW PASS_THROUGH queries.
 *
 */
public class QueryValidator {
    private static final Map<DistributionType, Schema> DISTRIBUTION_SCHEMAS = new HashMap<>();

    static {
        DISTRIBUTION_SCHEMAS.put(DistributionType.QUANTILE,
                new Schema(Arrays.asList(new Schema.PlainField(QuantileSketch.VALUE_FIELD, Type.DOUBLE),
                                         new Schema.PlainField(QuantileSketch.QUANTILE_FIELD, Type.DOUBLE))));
        DISTRIBUTION_SCHEMAS.put(DistributionType.PMF,
                new Schema(Arrays.asList(new Schema.PlainField(QuantileSketch.PROBABILITY_FIELD, Type.DOUBLE),
                                         new Schema.PlainField(QuantileSketch.COUNT_FIELD, Type.DOUBLE),
                                         new Schema.PlainField(QuantileSketch.RANGE_FIELD, Type.STRING))));
        DISTRIBUTION_SCHEMAS.put(DistributionType.CDF,
                new Schema(Arrays.asList(new Schema.PlainField(QuantileSketch.PROBABILITY_FIELD, Type.DOUBLE),
                                         new Schema.PlainField(QuantileSketch.COUNT_FIELD, Type.DOUBLE),
                                         new Schema.PlainField(QuantileSketch.RANGE_FIELD, Type.STRING))));
    }

    public static List<BulletError> validate(ProcessedQuery processedQuery, Query query, Schema baseSchema) {
        LayeredSchema schema = new LayeredSchema(baseSchema);
        ExpressionValidator expressionValidator = new ExpressionValidator(processedQuery, schema);

        // If there's a WHERE clause, check that it can be evaluated as a boolean.
        if (processedQuery.getWhereNode() != null) {
            Type type = expressionValidator.process(processedQuery.getWhereNode(), processedQuery.getPreAggregationMapping());
            if (!Type.isUnknown(type) && !Type.canForceCast(Type.BOOLEAN, type)) {
                processedQuery.getErrors().add(new BulletError("WHERE clause cannot be casted to BOOLEAN: " + processedQuery.getWhereNode(),
                                                               "Please specify a valid WHERE clause."));
            }
        }

        if (processedQuery.getProjection() != null) {
            expressionValidator.process(processedQuery.getProjection(), processedQuery.getPreAggregationMapping());
            List<Schema.Field> fields = toSchemaFields(query.getProjection().getFields());
            duplicates(fields).ifPresent(duplicates -> {
                processedQuery.getErrors().add(new BulletError("The following field names/aliases are shared: " + duplicates,
                                                               "Please specify non-overlapping field names and aliases."));
            });
            if (query.getProjection().getType() == Projection.Type.COPY) {
                schema.addLayer(new Schema(fields));
            } else {
                schema.replaceSchema(new Schema(fields));
            }
        }

        // Process group by and aggregate nodes. Order doesn't matter. It only matters that they're using the schema after projections.
        // If the query type is select distinct, the select nodes are the actual aggregate nodes.
        ProcessedQuery.QueryType queryType = processedQuery.getQueryType();

        if (queryType == ProcessedQuery.QueryType.SELECT_DISTINCT) {
            List<ExpressionNode> selectItems = processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).collect(Collectors.toList());
            expressionValidator.process(selectItems, processedQuery.getPreAggregationMapping());
            // Primitives only
            for (ExpressionNode node : selectItems) {
                Type type = processedQuery.getPreAggregationMapping().get(node).getType();
                if (!Type.isPrimitive(type)) {
                    processedQuery.getErrors().add(new BulletError("The SELECT DISTINCT field " + node + " is non-primitive. Type given: " + type,
                                                                   "Please specify primitive fields only for SELECT DISTINCT."));
                }
            }
        } else {
            expressionValidator.process(processedQuery.getAggregateNodes(), processedQuery.getPreAggregationMapping());
            expressionValidator.process(processedQuery.getGroupByNodes(), processedQuery.getPreAggregationMapping());
            // Primitives only
            for (ExpressionNode node : processedQuery.getGroupByNodes()) {
                Type type = processedQuery.getPreAggregationMapping().get(node).getType();
                if (!Type.isPrimitive(type)) {
                    processedQuery.getErrors().add(new BulletError("The GROUP BY field " + node + " is non-primitive. Type given: " + type,
                                                                   "Please specify primitive fields only for GROUP BY."));
                }
            }

        }

        // Replace schema with aggregate fields
        List<Schema.Field> aggregateFields = null;
        switch (queryType) {
            case SELECT_DISTINCT:
                aggregateFields = processedQuery.getSelectNodes().stream()
                                                                 .map(SelectItemNode::getExpression)
                                                                 .map(toSchemaField(processedQuery))
                                                                 .collect(Collectors.toList());
                break;
            case GROUP:
                aggregateFields = Stream.concat(processedQuery.getAggregateNodes().stream(),
                                                processedQuery.getGroupByNodes().stream())
                                        .map(toSchemaField(processedQuery))
                                        .collect(Collectors.toList());
                break;
            case COUNT_DISTINCT:
                aggregateFields = toSchemaFields(processedQuery.getCountDistinct(), processedQuery);
                break;
            case DISTRIBUTION:
                schema.replaceSchema(DISTRIBUTION_SCHEMAS.get(((Distribution) query.getAggregation()).getDistributionType()));
                break;
            case TOP_K:
                aggregateFields = toSchemaFields(processedQuery.getTopK().getExpressions(), processedQuery);
                aggregateFields.add(new Schema.PlainField(((TopK) query.getAggregation()).getName(), Type.LONG));
                break;
            case SPECIAL_K:
                aggregateFields = toSchemaFields(processedQuery.getGroupByNodes(), processedQuery);
                aggregateFields.add(new Schema.PlainField(((TopK) query.getAggregation()).getName(), Type.LONG));
                break;
        }
        if (aggregateFields != null) {
            duplicates(aggregateFields).ifPresent(duplicates -> {
                processedQuery.getErrors().add(new BulletError("The following field names/aliases are shared: " + duplicates,
                                                               "Please specify non-overlapping field names and aliases."));
            });
            schema.replaceSchema(new Schema(aggregateFields));
        }

        if (query.getPostAggregations() == null) {
            return processedQuery.getErrors();
        }

        Type type;
        for (PostAggregation postAggregation : query.getPostAggregations()) {
            switch (postAggregation.getType()) {
                case HAVING:
                    type = expressionValidator.process(processedQuery.getHavingNode(), processedQuery.getPostAggregationMapping());
                    if (!Type.isUnknown(type) && !Type.canForceCast(Type.BOOLEAN, type)) {
                        processedQuery.getErrors().add(new BulletError("HAVING clause cannot be casted to BOOLEAN: " + processedQuery.getHavingNode(),
                                                                       "Please specify a valid HAVING clause."));
                    }
                    break;
                case COMPUTATION:
                    expressionValidator.process(processedQuery.getComputation(), processedQuery.getPostAggregationMapping());
                    List<Schema.Field> fields = toSchemaFields(((Computation) postAggregation).getFields());
                    duplicates(fields).ifPresent(duplicates -> {
                        processedQuery.getErrors().add(new BulletError("The following field names/aliases are shared: " + duplicates,
                                                                       "Please specify non-overlapping field names and aliases."));
                    });
                    schema.addLayer(new Schema(fields));
                    break;
                case ORDER_BY:
                    for (OrderBy.SortItem sortItem : ((OrderBy) postAggregation).getFields()) {
                        type = schema.getType(sortItem.getField());
                        if (!Type.isUnknown(type)) {
                            if (Type.isNull(type)) {
                                processedQuery.getErrors().add(new BulletError("ORDER BY contains a non-existent field: " + sortItem.getField(), "Please specify an existing field."));
                            } else if (!Type.isPrimitive(type)) {
                                processedQuery.getErrors().add(new BulletError("ORDER BY contains a non-primitive field: " + sortItem.getField(), "Please specify a primitive field."));
                            }
                        }
                    }
                    break;
            }
        }

        return processedQuery.getErrors();
    }

    private static Optional<Set<String>> duplicates(List<Schema.Field> fields) {
        Map<String, Long> fieldsToCount = fields.stream().map(Schema.Field::getName).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        Set<String> duplicates = fieldsToCount.keySet().stream().filter(s -> fieldsToCount.get(s) > 1).collect(Collectors.toSet());
        return !duplicates.isEmpty() ? Optional.of(duplicates) : Optional.empty();
    }

    private static List<Schema.Field> toSchemaFields(List<Field> fields) {
        return fields.stream().map(field -> new Schema.PlainField(field.getName(), field.getValue().getType())).collect(Collectors.toList());
    }

    private static List<Schema.Field> toSchemaFields(List<ExpressionNode> expressions, ProcessedQuery processedQuery) {
        return expressions.stream().map(toSchemaField(processedQuery)).collect(Collectors.toCollection(ArrayList::new));
    }

    private static List<Schema.Field> toSchemaFields(ExpressionNode expression, ProcessedQuery processedQuery) {
        return Collections.singletonList(new Schema.PlainField(processedQuery.getAliasOrName(expression), processedQuery.getPreAggregationMapping().get(expression).getType()));
    }

    private static Function<ExpressionNode, Schema.Field> toSchemaField(ProcessedQuery processedQuery) {
        return node -> new Schema.PlainField(processedQuery.getAliasOrName(node), processedQuery.getPreAggregationMapping().get(node).getType());
    }
}
