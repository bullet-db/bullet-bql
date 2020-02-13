/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.TopK;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Culling;
import com.yahoo.bullet.parsing.OrderBy;
import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.parsing.Query;
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

public class QueryValidator {
    private static final Map<Distribution.Type, Schema> DISTRIBUTION_SCHEMAS = new HashMap<>();

    static {
        DISTRIBUTION_SCHEMAS.put(Distribution.Type.QUANTILE,
                new Schema(Arrays.asList(new Schema.Field("Value", Type.DOUBLE),
                                         new Schema.Field("Quantile", Type.DOUBLE))));
        DISTRIBUTION_SCHEMAS.put(Distribution.Type.PMF,
                new Schema(Arrays.asList(new Schema.Field("Probability", Type.DOUBLE),
                                         new Schema.Field("Count", Type.DOUBLE),
                                         new Schema.Field("Range", Type.STRING))));
        DISTRIBUTION_SCHEMAS.put(Distribution.Type.CDF,
                new Schema(Arrays.asList(new Schema.Field("Probability", Type.DOUBLE),
                                         new Schema.Field("Count", Type.DOUBLE),
                                         new Schema.Field("Range", Type.STRING))));
    }

    public static List<BulletError> validate(ProcessedQuery processedQuery, Query query, Schema baseSchema) {
        ExpressionValidator expressionValidator = new ExpressionValidator(processedQuery);
        LayeredSchema schema = new LayeredSchema(baseSchema);

        // If there's a WHERE clause, check that it can be evaluated as a boolean.
        if (processedQuery.getWhereNode() != null) {
            Type type = expressionValidator.process(processedQuery.getWhereNode(), schema);
            if (!Type.isUnknown(type) && !Type.canCast(Type.BOOLEAN, type)) {
                processedQuery.getErrors().add(new BulletError("WHERE clause cannot be casted to BOOLEAN: " + processedQuery.getWhereNode(), null));
            }
        }

        if (query.getProjection() != null) {
            expressionValidator.process(processedQuery.getProjectionNodes(), schema);
            List<Schema.Field> fields = query.getProjection().getFields().stream().map(field -> new Schema.Field(field.getName(), field.getValue().getType())).collect(Collectors.toList());
            duplicates(fields).ifPresent(duplicates -> {
                    processedQuery.getErrors().add(new BulletError("The following field names/aliases are shared: " + duplicates, null));
                });
            if (query.getProjection().isCopy()) {
                schema.addLayer(new Schema(fields));
            } else {
                schema.replaceSchema(new Schema(fields));
            }
        }

        // Process group by and aggregate nodes. Order doesn't matter. It only matters that they're using the schema after projections.
        // If the query type is select distinct, the select nodes are the actual aggregate nodes.
        ProcessedQuery.QueryType queryType = processedQuery.getQueryType();

        if (queryType == ProcessedQuery.QueryType.SELECT_DISTINCT) {
            processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression).forEach(node -> expressionValidator.process(node, schema));
        } else {
            expressionValidator.process(processedQuery.getAggregateNodes(), schema);
            expressionValidator.process(processedQuery.getGroupByNodes(), schema);
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
                aggregateFields = toSchemaFieldList(processedQuery.getCountDistinct(), processedQuery);
                break;
            case DISTRIBUTION:
                schema.replaceSchema(DISTRIBUTION_SCHEMAS.get(query.getAggregation().getAttributes().get(Distribution.TYPE)));
                break;
            case TOP_K:
                aggregateFields = toSchemaFieldList(processedQuery.getTopK().getExpressions(), processedQuery);
                aggregateFields.add(new Schema.Field(getTopKName(query), Type.LONG));
                break;
            case SPECIAL_K:
                aggregateFields = toSchemaFieldList(processedQuery.getGroupByNodes(), processedQuery);
                aggregateFields.add(new Schema.Field(getTopKName(query), Type.LONG));
                break;
        }
        if (aggregateFields != null) {
            duplicates(aggregateFields).ifPresent(duplicates -> {
                    processedQuery.getErrors().add(new BulletError("The following field names/aliases are shared: " + duplicates, null));
                });
            schema.replaceSchema(new Schema(aggregateFields));
        }

        // If there's a HAVING clause, check that it can be evaluated as a boolean. The HAVING clause in special k queries doesn't count.
        if (processedQuery.getHavingNode() != null && queryType != ProcessedQuery.QueryType.SPECIAL_K) {
            Type type = expressionValidator.process(processedQuery.getHavingNode(), schema);
            if (!Type.isUnknown(type) && !Type.canCast(Type.BOOLEAN, type)) {
                processedQuery.getErrors().add(new BulletError("HAVING clause cannot be casted to BOOLEAN: " + processedQuery.getHavingNode(), null));
            }
        }

        if (query.getPostAggregations() != null) {
            Optional<PostAggregation> computation = query.getPostAggregations().stream().filter(Computation.class::isInstance).findFirst();
            if (computation.isPresent()) {
                processedQuery.getComputationNodes().forEach(node -> expressionValidator.process(node, schema));
                List<Schema.Field> fields = ((Computation) computation.get()).getFields().stream().map(field -> new Schema.Field(field.getName(), field.getValue().getType())).collect(Collectors.toList());
                duplicates(fields).ifPresent(duplicates -> {
                        processedQuery.getErrors().add(new BulletError("The following field names/aliases are shared: " + duplicates, null));
                    });
                schema.addLayer(new Schema(fields));
            }
            Optional<PostAggregation> orderBy = query.getPostAggregations().stream().filter(OrderBy.class::isInstance).findFirst();
            if (orderBy.isPresent()) {
                for (OrderBy.SortItem sortItem : ((OrderBy) orderBy.get()).getFields()) {
                    Type type = schema.getType(sortItem.getField());
                    if (!Type.isUnknown(type)) {
                        if (Type.isNull(type)) {
                            processedQuery.getErrors().add(new BulletError("ORDER BY contains a non-existent field: " + sortItem.getField(), null));
                        } else if (!Type.isPrimitive(type)) {
                            processedQuery.getErrors().add(new BulletError("ORDER BY contains a non-primitive field: " + sortItem.getField(), null));
                        }
                    }
                }
            }
            // TODO this part shouldn't be necessary... remove later
            Optional<PostAggregation> culling = query.getPostAggregations().stream().filter(Culling.class::isInstance).findFirst();
            if (culling.isPresent()) {
                for (String transientField : ((Culling) culling.get()).getTransientFields()) {
                    if (Type.isNull(schema.getType(transientField))) {
                        processedQuery.getErrors().add(new BulletError("Tries to cull a missing field (programming logic error): " + transientField, null));
                    }
                }
            }
        }

        return processedQuery.getErrors();
    }

    private static Optional<Set<String>> duplicates(List<Schema.Field> fields) {
        Map<String, Long> fieldsToCount = fields.stream().map(Schema.Field::getName).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        Set<String> duplicates = fieldsToCount.keySet().stream().filter(s -> fieldsToCount.get(s) > 1).collect(Collectors.toSet());
        return !duplicates.isEmpty() ? Optional.of(duplicates) : Optional.empty();
    }

    private static Function<ExpressionNode, Schema.Field> toSchemaField(ProcessedQuery processedQuery) {
        return node -> new Schema.Field(processedQuery.getAliasOrName(node), processedQuery.getExpression(node).getType());
    }

    private static List<Schema.Field> toSchemaFieldList(List<ExpressionNode> expressions, ProcessedQuery processedQuery) {
        return expressions.stream().map(toSchemaField(processedQuery)).collect(Collectors.toCollection(ArrayList::new));
    }

    private static List<Schema.Field> toSchemaFieldList(ExpressionNode expression, ProcessedQuery processedQuery) {
        return Collections.singletonList(new Schema.Field(processedQuery.getAliasOrName(expression), processedQuery.getExpression(expression).getType()));
    }

    private static String getTopKName(Query query) {
        if (query.getAggregation().getAttributes() == null) {
            return TopK.DEFAULT_NEW_NAME;
        }
        return (String) query.getAggregation().getAttributes().getOrDefault(TopK.NEW_NAME_FIELD, TopK.DEFAULT_NEW_NAME);
    }
}
