/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.aggregations.Distribution;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.aggregations.TopK;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.PostAggregation;
import com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The validator processes the types for all expressions in a query and checks if they're semantically valid, e.g.
 * a WHERE clause or HAVING clause should have an expression that can be casted to BOOLEAN. The validator also checks
 * for duplicate fields, so a query that clobbers its fields with aliases is invalid.
 *
 * Note: If no schema is given to the validator, most expressions will be processed as type UNKNOWN, which vacuously
 * passes through checks.
 *
 * Note: RAW PASS_THROUGH queries pass Bullet records as is through projection and aggregation. This means that BQL
 * should not generate post-aggregations such as computation and culling for these queries as these can modify
 * records.
 *
 * A RAW aggregation is only possible for SELECT and SELECT_ALL queries. Out of these two, only SELECT_ALL queries
 * can have a PASS_THROUGH projection. This means any RAW PASS_THROUGH query is also a SELECT_ALL query.
 * SELECT_ALL queries exit early in the culling extractor if they have a PASS_THROUGH projection (i.e. no
 * projection expression nodes in its ProcessedQuery), and SELECT_ALL queries exit early in the computation extractor.
 *
 * In this way, BQL cannot and does not generate computation and culling post-aggregations for RAW PASS_THROUGH queries.
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
    /*
    public static List<BulletError> validate(ProcessedQuery processedQuery, Query query, Schema baseSchema) {
        LayeredSchema schema = new LayeredSchema(baseSchema);
        ExpressionValidator expressionValidator = new ExpressionValidator(processedQuery, schema);
        ProcessedQuery.QueryType queryType = processedQuery.getQueryType();

        // Type check the select fields for a SELECT * query since there can be fields not covered by the projection.
        if (queryType == ProcessedQuery.QueryType.SELECT_ALL) {
            expressionValidator.process(processedQuery.getSelectNodes(), processedQuery.getPreAggregationMapping());
        }

        // Check that the filter clause can be evaluated as a boolean
        expressionValidator.process(processedQuery.getWhereNode(), processedQuery.getPreAggregationMapping());
        validateFilter(processedQuery);

        // Type check everything in SELECT exists or is proper
        expressionValidator.process(processedQuery.getProjection(), processedQuery.getPreAggregationMapping());
        validateProjection(processedQuery, schema, query);

        // Process group by and aggregate nodes. If the query type is select distinct, the select nodes are the aggregate nodes.
        if (queryType == ProcessedQuery.QueryType.SELECT_DISTINCT) {
            expressionValidator.process(processedQuery.getSelectNodes(), processedQuery.getPreAggregationMapping());
            validateSelectDistinct(processedQuery);
        } else {
            expressionValidator.process(processedQuery.getAggregateNodes(), processedQuery.getPreAggregationMapping());
            expressionValidator.process(processedQuery.getGroupByNodes(), processedQuery.getPreAggregationMapping());
            validateGroupBy(processedQuery);
        }

        // Replace schema with aggregation if it exists
        validateAggregation(processedQuery, schema, queryType, query);

        if (queryType != ProcessedQuery.QueryType.SELECT && queryType != ProcessedQuery.QueryType.SELECT_ALL) {
            // Set aliases allowed for actually existing aliases, e.g. the field "AVG(a)" is not a valid reference to the aggregate AVG(a).
            // This matters for group by, count distinct, top k, and special k queries to avoid an edge case
            expressionValidator.setAliases(getAliases(processedQuery, queryType));
            expressionValidator.process(getAdditionalSelectNodes(processedQuery, queryType), processedQuery.getPostAggregationMapping());
        }

        validatePostAggregations(processedQuery, expressionValidator, schema, query);

        return processedQuery.getErrors();
    }

    private static void validateFilter(ProcessedQuery processedQuery) {
        if (processedQuery.getWhereNode() != null) {
            Type type = processedQuery.getPreAggregationMapping().get(processedQuery.getWhereNode()).getType();
            if (!Type.isUnknown(type) && !Type.canForceCast(Type.BOOLEAN, type)) {
                processedQuery.getErrors().add(new BulletError(processedQuery.getWhereNode().getLocation() + "WHERE clause cannot be casted to BOOLEAN: " + processedQuery.getWhereNode(),
                                                               "Please specify a valid WHERE clause."));
            }
        }
    }

    private static void validateProjection(ProcessedQuery processedQuery, LayeredSchema schema, Query query) {
        if (processedQuery.getProjection() != null) {
            List<Schema.Field> fields = toSchemaFields(query.getProjection().getFields());
            duplicates(fields).ifPresent(duplicates -> {
                processedQuery.getErrors().add(new BulletError("The following field names are shared: " + duplicates,
                                                               "Please specify non-overlapping field names."));
            });
            if (query.getProjection().getType() == Projection.Type.COPY) {
                schema.addLayer(new Schema(fields));
            } else {
                schema.replaceSchema(new Schema(fields));
            }
        }
    }

    private static void validateSelectDistinct(ProcessedQuery processedQuery) {
        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            Type type = processedQuery.getPreAggregationMapping().get(node).getType();
            if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                processedQuery.getErrors().add(new BulletError(node.getLocation() + "The SELECT DISTINCT field " + node + " is non-primitive. Type given: " + type,
                                                               "Please specify primitive fields only for SELECT DISTINCT."));
            }
        }
    }

    private static void validateGroupBy(ProcessedQuery processedQuery) {
        for (ExpressionNode node : processedQuery.getGroupByNodes()) {
            Type type = processedQuery.getPreAggregationMapping().get(node).getType();
            if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                processedQuery.getErrors().add(new BulletError(node.getLocation() + "The GROUP BY field " + node + " is non-primitive. Type given: " + type,
                                                               "Please specify primitive fields only for GROUP BY."));
            }
        }
    }

    private static void validateAggregation(ProcessedQuery processedQuery, LayeredSchema schema, ProcessedQuery.QueryType queryType, Query query) {
        List<Schema.Field> aggregateFields = null;
        switch (queryType) {
            case SELECT_DISTINCT:
                aggregateFields = processedQuery.getSelectNodes().stream().map(toSchemaField(processedQuery)).collect(Collectors.toList());
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
    }

    private static Set<ExpressionNode> getAdditionalSelectNodes(ProcessedQuery processedQuery, ProcessedQuery.QueryType queryType) {
        switch (queryType) {
            case GROUP:
                return processedQuery.getSelectNodes().stream()
                                                      .filter(processedQuery::isSimpleFieldExpression)
                                                      .filter(processedQuery::isNotGroupByNode)
                                                      .collect(Collectors.toSet());
            case DISTRIBUTION:
                return processedQuery.getSelectNodes().stream()
                                                      .filter(processedQuery::isSimpleFieldExpression)
                                                      .collect(Collectors.toSet());
            case COUNT_DISTINCT:
                List<ExpressionNode> countDistinctExpressions = processedQuery.getCountDistinct().getExpressions();
                return processedQuery.getSelectNodes().stream()
                                                      .filter(processedQuery::isSimpleFieldExpression)
                                                      .filter(node -> !countDistinctExpressions.contains(node))
                                                      .collect(Collectors.toSet());
            case TOP_K:
                List<ExpressionNode> topKExpressions = processedQuery.getTopK().getExpressions();
                return processedQuery.getSelectNodes().stream()
                                                      .filter(processedQuery::isSimpleFieldExpression)
                                                      .filter(node -> !topKExpressions.contains(node))
                                                      .collect(Collectors.toSet());
            case SPECIAL_K:
                return processedQuery.getSelectNodes().stream()
                                                      .filter(processedQuery::isSimpleFieldExpression)
                                                      .filter(processedQuery::isNotGroupByNode)
                                                      .collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }

    private static Set<String> getAliases(ProcessedQuery processedQuery, ProcessedQuery.QueryType queryType) {
        Set<String> aliases;
        String alias;
        switch (queryType) {
            case SELECT_DISTINCT:
                return processedQuery.getSelectNodes().stream()
                        .filter(node -> processedQuery.isSimpleFieldExpression(node) || processedQuery.hasAlias(node))
                        .map(processedQuery::getAliasOrName)
                        .collect(Collectors.toSet());
            case GROUP:
                return Stream.concat(processedQuery.getGroupByNodes().stream(),
                                     processedQuery.getGroupOpNodes().stream())
                             .filter(node -> processedQuery.isSimpleFieldExpression(node) || processedQuery.hasAlias(node))
                             .map(processedQuery::getAliasOrName)
                             .collect(Collectors.toSet());
            case COUNT_DISTINCT:
                alias = processedQuery.getAlias(processedQuery.getCountDistinct());
                return alias != null ? Collections.singleton(alias) : Collections.emptySet();
            case TOP_K:
                TopKNode topK = processedQuery.getTopK();
                aliases = new HashSet<>(Collections.singleton(processedQuery.getAlias(topK)));
                aliases.addAll(topK.getExpressions().stream()
                                                    .filter(node -> processedQuery.isSimpleFieldExpression(node) || processedQuery.hasAlias(node))
                                                    .map(processedQuery::getAliasOrName)
                                                    .collect(Collectors.toSet()));
                return aliases;
            case SPECIAL_K:
                ExpressionNode count = processedQuery.getGroupOpNodes().iterator().next();
                aliases = new HashSet<>(Collections.singleton(processedQuery.getAliasOrName(count)));
                aliases.addAll(processedQuery.getGroupByNodes().stream()
                                                               .filter(node -> processedQuery.isSimpleFieldExpression(node) || processedQuery.hasAlias(node))
                                                               .map(processedQuery::getAliasOrName)
                                                               .collect(Collectors.toSet()));
                return aliases;
        }
        return null;
    }

    private static void validatePostAggregations(ProcessedQuery processedQuery, ExpressionValidator expressionValidator, LayeredSchema schema, Query query) {
        if (query.getPostAggregations() == null) {
            return;
        }
        Type type;
        for (PostAggregation postAggregation : query.getPostAggregations()) {
            switch (postAggregation.getType()) {
                case HAVING:
                    expressionValidator.process(processedQuery.getHavingNode(), processedQuery.getPostAggregationMapping());
                    type = processedQuery.getPostAggregationMapping().get(processedQuery.getHavingNode()).getType();
                    if (!Type.isUnknown(type) && !Type.canForceCast(Type.BOOLEAN, type)) {
                        processedQuery.getErrors().add(new BulletError(processedQuery.getHavingNode().getLocation() + "HAVING clause cannot be casted to BOOLEAN: " + processedQuery.getHavingNode(),
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
                    expressionValidator.process(processedQuery.getOrderByNodes(), processedQuery.getPostAggregationMapping());
                    for (ExpressionNode orderByNode : processedQuery.getOrderByNodes()) {
                        type = processedQuery.getPostAggregationMapping().get(orderByNode).getType();
                        if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                            processedQuery.getErrors().add(new BulletError(orderByNode.getLocation() + "ORDER BY contains a non-primitive field: " + orderByNode, "Please specify a primitive field."));
                        }
                    }
                    break;
            }
        }
    }

    private static Optional<Set<String>> duplicates(List<Schema.Field> fields) {
        Map<String, Long> fieldsToCount = fields.stream().map(Schema.Field::getName).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        Set<String> duplicates = fieldsToCount.keySet().stream().filter(s -> fieldsToCount.get(s) > 1).collect(Collectors.toSet());
        return !duplicates.isEmpty() ? Optional.of(duplicates) : Optional.empty();
    }

    private static List<Schema.Field> toSchemaFields(List<Field> fields) {
        return fields.stream().map(field -> new Schema.PlainField(field.getName(), field.getValue().getType())).collect(Collectors.toList());
    }

    private static List<Schema.Field> toSchemaFields(Collection<ExpressionNode> expressions, ProcessedQuery processedQuery) {
        return expressions.stream().map(toSchemaField(processedQuery)).collect(Collectors.toCollection(ArrayList::new));
    }

    private static List<Schema.Field> toSchemaFields(ExpressionNode expression, ProcessedQuery processedQuery) {
        return Collections.singletonList(new Schema.PlainField(processedQuery.getAliasOrName(expression), processedQuery.getPreAggregationMapping().get(expression).getType()));
    }

    private static Function<ExpressionNode, Schema.Field> toSchemaField(ProcessedQuery processedQuery) {
        return node -> new Schema.PlainField(processedQuery.getAliasOrName(node), processedQuery.getPreAggregationMapping().get(node).getType());
    }
    */
}
