/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.WindowIncludeNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.aggregations.CountDistinct;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.aggregations.GroupAll;
import com.yahoo.bullet.query.aggregations.GroupBy;
import com.yahoo.bullet.query.aggregations.Raw;
import com.yahoo.bullet.query.aggregations.TopK;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.query.postaggregations.Having;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.query.postaggregations.PostAggregation;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.querying.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryBuilder {
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

    private ProcessedQuery processedQuery;
    private Projection projection;
    private Expression filter;
    private Aggregation aggregation;
    private Window window;
    private Long duration;
    private Integer limit;

    private Set<Field> projectionFields = new LinkedHashSet<>();
    private Set<Field> computationFields = new LinkedHashSet<>();

    private boolean requiresCopyFlag;
    private boolean requiresNoCopyFlag;

    @Getter
    private List<BulletError> errors = new ArrayList<>();
    private List<PostAggregation> postAggregations = new ArrayList<>();

    @Getter
    private Query query;

    private LayeredSchema layeredSchema;

    // Used to build layers
    private Schema schema = new Schema();
    private Map<String, String> aliases = new HashMap<>();

    private ExpressionVisitor expressionVisitor = new ExpressionVisitor(errors);

    public QueryBuilder(ProcessedQuery processedQuery, Schema schema) {
        this.processedQuery = processedQuery;
        this.layeredSchema = new LayeredSchema(schema);
        buildQuery();
    }

    private void buildQuery() {
        doCommon();
        switch (processedQuery.getQueryType()) {
            case SELECT:
                doSelect();
                break;
            case SELECT_ALL:
                doSelectAll();
                break;
            case SELECT_DISTINCT:
                doSelectDistinct();
                break;
            case GROUP:
                if (processedQuery.isSpecialK()) {
                    doSpecialK();
                } else {
                    doGroup();
                }
                break;
            case COUNT_DISTINCT:
                doCountDistinct();
                break;
            case DISTRIBUTION:
                doDistribution();
                break;
            case TOP_K:
                doTopK();
                break;
        }
        if (hasErrors()) {
            return;
        }
        query = new Query(projection, filter, aggregation, !postAggregations.isEmpty() ? postAggregations : null, window, duration);
    }

    private void doCommon() {
        window = getWindow(processedQuery.getWindow());
        duration = processedQuery.getDuration();
        limit = processedQuery.getLimit();

        ExpressionNode whereNode = processedQuery.getWhere();
        if (whereNode != null) {
            filter = visit(processedQuery.getWhere());
            if (cannotCastToBoolean(filter.getType())) {
                addError(whereNode, QueryError.WHERE_CANNOT_CAST_TO_BOOLEAN, whereNode);
            }
        }
    }

    private void doSelect() {
        doSelectFields();

        requiresNoCopyFlag = true;

        addSchemaLayer(true);

        aggregation = new Raw(limit);

        // Missing fields
        Set<String> additionalFields = OrderByProcessor.visit(processedQuery.getOrderByNodes(), layeredSchema);

        for (String field : additionalFields) {
            FieldExpression expression = new FieldExpression(field);
            expression.setType(layeredSchema.getType(field));
            addProjectionField(field, expression);
        }

        doOrderBy();

        if (!additionalFields.isEmpty()) {
            postAggregations.add(new Culling(additionalFields));
        }

        // Create projection at the end because ORDER BY can add additional fields
        doProjection();
    }

    private void doSelectAll() {
        doSelectFields();

        requiresCopyFlag = processedQuery.getSelectNodes().stream().anyMatch(node -> !(node instanceof FieldExpressionNode) || processedQuery.hasAlias(node));

        doProjection();

        addSchemaLayer(requiresCopyFlag);

        aggregation = new Raw(limit);

        doOrderBy();

        // Renamed fields that are not in the final schema are removed
        if (requiresCopyFlag) {
            Set<String> transientFields = layeredSchema.getExtraneousAliases();
            if (!transientFields.isEmpty()) {
                postAggregations.add(new Culling(transientFields));
            }
        }
    }

    private void doSelectDistinct() {
        Map<String, String> fields = new HashMap<>();
        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            Expression expression = visit(node);
            String newName = processedQuery.getAliasOrName(node);
            String name = node.getName();
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                addAlias(name, newName);
            }
            addProjectionField(name, expression);
            addSchemaField(newName, type);

            fields.put(name, newName);

            if (isNonPrimitive(type)) {
                addError(node, QueryError.SELECT_DISTINCT_FIELD_NON_PRIMITIVE, node, type);
            }
        }

        requiresNoCopyFlag = processedQuery.getSelectNodes().stream().anyMatch(node -> !(node instanceof FieldExpressionNode));

        doProjection();

        addSchemaLayer(true);

        // Check for duplicate aggregate names
        checkDuplicates(fields.values());

        aggregation = new GroupBy(limit, fields, Collections.emptySet());

        doOrderBy();
    }

    private void doGroup() {
        Map<String, String> fields = new HashMap<>();

        List<String> schemaFields = new ArrayList<>();

        for (ExpressionNode node : processedQuery.getGroupByNodes()) {
            Expression expression = visit(node);
            String newName = processedQuery.getAliasOrName(node);
            String name = node.getName();
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                addAlias(name, newName);
            }
            addProjectionField(name, expression);
            addSchemaField(newName, type);

            fields.put(name, newName);
            schemaFields.add(newName);

            if (!(node instanceof FieldExpressionNode)) {
                requiresNoCopyFlag = true;
            }
            if (isNonPrimitive(type)) {
                addError(node, QueryError.GROUP_BY_FIELD_NON_PRIMITIVE, node, type);
            }
        }

        Set<GroupOperation> operations = new HashSet<>();

        for (GroupOperationNode node : processedQuery.getGroupOpNodes()) {
            Expression expression = visit(node);
            String newName = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            // Mapping for the aggregate
            if (processedQuery.hasAlias(node)) {
                addAlias(node.getName(), newName);
            }
            addSchemaField(newName, type);

            schemaFields.add(newName);

            ExpressionNode expressionNode = node.getExpression();
            if (expressionNode != null) {
                //addProjectionField(expressionNode.getName(), ExpressionVisitor.visit(expressionNode, querySchema));
                addProjectionField(expressionNode.getName(), visit(expressionNode));
                operations.add(new GroupOperation(node.getOp(), expressionNode.getName(), newName));
                if (!(expressionNode instanceof FieldExpressionNode)) {
                    requiresNoCopyFlag = true;
                }
            } else {
                operations.add(new GroupOperation(node.getOp(), null, newName));
            }
        }

        doProjection();

        // Check for duplicate aggregate names
        checkDuplicates(schemaFields);

        if (!fields.isEmpty()) {
            aggregation = new GroupBy(limit, fields, operations);
        } else {
            aggregation = new GroupAll(operations);
        }

        addSchemaLayer(true);

        ExpressionNode having = processedQuery.getHaving();
        if (having != null) {
            Expression expression = visit(having);
            Type type = expression.getType();
            if (cannotCastToBoolean(type)) {
                addError(having, QueryError.HAVING_CANNOT_CAST_TO_BOOLEAN, having);
            }
            postAggregations.add(new Having(expression));
        }

        doComputation();
        doOrderBy();
        doTransient();
    }

    private void doCountDistinct() {
        CountDistinctNode countDistinctNode = processedQuery.getCountDistinct();
        List<ExpressionNode> countDistinctExpressions = countDistinctNode.getExpressions();

        List<String> fields = new ArrayList<>();

        for (ExpressionNode node : countDistinctExpressions) {
            Expression expression = visit(node);
            String name = node.getName();
            addProjectionField(name, expression);
            fields.add(name);
        }

        requiresNoCopyFlag = countDistinctExpressions.stream().anyMatch(node -> !(node instanceof FieldExpressionNode));

        doProjection();

        Expression countDistinctExpression = visit(countDistinctNode);
        String countDistinctName = processedQuery.getAliasOrName(countDistinctNode);
        if (processedQuery.hasAlias(countDistinctNode)) {
            addAlias(countDistinctNode.getName(), countDistinctName);
        }
        addSchemaField(countDistinctName, countDistinctExpression.getType());

        addSchemaLayer(true);

        // No need to check for duplicates in aggregation because only one field (count distinct)
        aggregation = new CountDistinct(fields, countDistinctName);

        doComputation();
        doTransient();
    }

    private void doDistribution() {
        DistributionNode distributionNode = processedQuery.getDistribution();

        ExpressionNode distributionExpressionNode = distributionNode.getExpression();
        Expression distributionExpression = visit(distributionExpressionNode);
        String distributionExpressionName = distributionExpressionNode.getName();

        addProjectionField(distributionExpressionName, distributionExpression);

        requiresNoCopyFlag = !(distributionExpressionNode instanceof FieldExpressionNode);

        doProjection();

        visit(distributionNode);

        schema = DISTRIBUTION_SCHEMAS.get(distributionNode.getType());

        addSchemaLayer(true);

        // No need to check for duplicates in aggregation because fixed fields
        aggregation = distributionNode.getAggregation(limit);

        doComputation();
        doOrderBy();
    }

    private void doTopK() {
        TopKNode topKNode = processedQuery.getTopK();
        List<ExpressionNode> topKExpressions = topKNode.getExpressions();
        Map<String, String> fields = new HashMap<>();

        for (ExpressionNode node : topKExpressions) {
            Expression expression = visit(node);
            Type type = expression.getType();
            String name = node.getName();
            String newName = processedQuery.getAliasOrName(node);
            if (processedQuery.hasAlias(node)) {
                addAlias(name, newName);
            }
            addProjectionField(name, expression);
            addSchemaField(newName, type);
            fields.put(name, newName);
        }

        requiresNoCopyFlag = topKExpressions.stream().anyMatch(node -> !(node instanceof FieldExpressionNode));

        doProjection();

        visit(topKNode);
        String topKAlias = processedQuery.getAlias(topKNode);
        addSchemaField(topKAlias, Type.LONG);

        addSchemaLayer(true);

        // Check for duplicate aggregate names
        checkDuplicates(fields.values());

        aggregation = new TopK(fields, topKNode.getSize(), topKNode.getThreshold(), topKAlias);

        doComputation();
    }

    private void doSpecialK() {
        Map<String, String> fields = new HashMap<>();

        for (ExpressionNode node : processedQuery.getGroupByNodes()) {
            Expression expression = visit(node);
            Type type = expression.getType();
            String name = node.getName();
            String newName = processedQuery.getAliasOrName(node);
            if (processedQuery.hasAlias(node)) {
                addAlias(name, newName);
            }
            addProjectionField(name, expression);
            addSchemaField(newName, type);
            fields.put(name, newName);
            if (isNonPrimitive(type)) {
                addError(node, QueryError.GROUP_BY_FIELD_NON_PRIMITIVE, node, type);
            }
        }

        requiresNoCopyFlag = processedQuery.getGroupByNodes().stream().anyMatch(node -> !(node instanceof FieldExpressionNode));

        doProjection();

        ExpressionNode countNode = processedQuery.getGroupOpNodes().iterator().next();
        visit(countNode);
        String countAliasOrName = processedQuery.getAliasOrName(countNode);
        if (processedQuery.hasAlias(countNode)) {
            addAlias(countNode.getName(), countAliasOrName);
        }
        addSchemaField(countAliasOrName, Type.LONG);

        addSchemaLayer(true);

        Long threshold = null;
        if (processedQuery.getHaving() != null) {
            threshold = ((Number) ((LiteralNode) ((BinaryExpressionNode) processedQuery.getHaving()).getRight()).getValue()).longValue();
        }

        // Check for duplicate aggregate names
        checkDuplicates(fields.values());

        aggregation = new TopK(fields, limit, threshold, countAliasOrName);

        doComputation();
    }

    private void doSelectFields() {
        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            Expression expression = visit(node);
            String newName = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                addAlias(node.getName(), newName);
            }
            addProjectionField(newName, expression);
            addSchemaField(newName, type);
        }
    }

    private void doProjection() {
        if (requiresCopyFlag) {
            projection = new Projection(new ArrayList<>(projectionFields), true);
        } else if (requiresNoCopyFlag) {
            projection = new Projection(new ArrayList<>(projectionFields), false);
        } else {
            projection = new Projection();
        }
        checkDuplicates(projectionFields.stream().map(Field::getName).collect(Collectors.toList()));
    }

    private void doComputation() {
        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            if (layeredSchema.hasField(node.getName())) {
                continue;
            }
            Expression expression = visit(node);
            String newName = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                addAlias(node.getName(), newName);
            }
            addComputationField(newName, expression);
            addSchemaField(newName, type);
        }
        if (!computationFields.isEmpty()) {
            postAggregations.add(new Computation(new ArrayList<>(computationFields)));
        }
        addSchemaLayer(false);
        checkDuplicates(computationFields.stream().map(Field::getName).collect(Collectors.toList()));
    }

    private void doOrderBy() {
        if (processedQuery.getSortItems().isEmpty()) {
            return;
        }
        List<OrderBy.SortItem> sortItems = new ArrayList<>();
        for (SortItemNode sortItem : processedQuery.getSortItems()) {
            ExpressionNode orderByNode = sortItem.getExpression();
            Expression expression = visit(orderByNode);
            if (isNonPrimitive(expression.getType())) {
                addError(orderByNode, QueryError.ORDER_BY_FIELD_NON_PRIMITIVE, orderByNode);
            }
            sortItems.add(new OrderBy.SortItem(expression, sortItem.getOrdering().getDirection()));
        }
        postAggregations.add(new OrderBy(sortItems));
    }

    private void doTransient() {
        Set<String> transientFields = layeredSchema.getFieldNames();
        processedQuery.getSelectNodes().stream().map(processedQuery::getAliasOrName).forEach(transientFields::remove);
        if (!transientFields.isEmpty()) {
            postAggregations.add(new Culling(transientFields));
        }
    }

    private Expression visit(ExpressionNode node) {
        return expressionVisitor.process(node, layeredSchema);
    }

    private void addProjectionField(String name, Expression expression) {
        projectionFields.add(new Field(name, expression));
    }

    private void addComputationField(String name, Expression expression) {
        computationFields.add(new Field(name, expression));
    }

    private void addSchemaField(String name, Type type) {
        schema.addField(name, type);
    }

    private void addAlias(String name, String alias) {
        aliases.put(name, alias);
    }

    private void addSchemaLayer(boolean lockSchema) {
        layeredSchema.addLayer(schema, aliases);
        schema = new Schema();
        aliases = new HashMap<>();
        expressionVisitor.resetMapping();
        if (lockSchema) {
            layeredSchema.lock();
        }
    }

    private static Window getWindow(WindowNode windowNode) {
        if (windowNode == null) {
            return new Window();
        }
        WindowIncludeNode windowInclude = windowNode.getWindowInclude();
        if (windowInclude == null) {
            return new Window(windowNode.getEmitEvery(), windowNode.getEmitType());
        }
        return new Window(windowNode.getEmitEvery(), windowNode.getEmitType(), windowInclude.getIncludeUnit(), windowInclude.getFirst());
    }

    private static Optional<List<String>> duplicates(Collection<String> fields) {
        Map<String, Long> fieldsToCount = fields.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        List<String> duplicates = fieldsToCount.keySet().stream().filter(s -> fieldsToCount.get(s) > 1).collect(Collectors.toList());
        return !duplicates.isEmpty() ? Optional.of(duplicates) : Optional.empty();
    }

    private static boolean cannotCastToBoolean(Type type) {
        return !Type.isUnknown(type) && !Type.canForceCast(Type.BOOLEAN, type);
    }

    private static boolean isNonPrimitive(Type type) {
        return !Type.isUnknown(type) && !Type.isPrimitive(type);
    }

    private void checkDuplicates(Collection<String> fields) {
        duplicates(fields).ifPresent(duplicates ->
            addError(null, QueryError.DUPLICATE_FIELD_NAMES_ALIASES, duplicates)
        );
    }

    private void addError(ExpressionNode node, QueryError queryError, Object... args) {
        errors.add(node == null ? queryError.format(args) : queryError.format(node.getLocation(), args));
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }
}
