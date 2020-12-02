package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.aggregations.CountDistinct;
import com.yahoo.bullet.query.aggregations.GroupAll;
import com.yahoo.bullet.query.aggregations.GroupBy;
import com.yahoo.bullet.query.aggregations.Raw;
import com.yahoo.bullet.query.aggregations.TopK;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.query.postaggregations.Having;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.query.postaggregations.PostAggregation;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryBuilder {
    private ProcessedQuery processedQuery;
    private QueryNode queryNode;
    private Projection projection;
    private Expression filter;
    private Aggregation aggregation;
    private Window window;
    private Long duration;
    private Integer limit;

    private QuerySchema querySchema;
    private List<BulletError> errors = new ArrayList<>();
    private List<PostAggregation> postAggregations = new ArrayList<>();

    @Getter
    private Query query;

    public QueryBuilder(QueryNode queryNode, ProcessedQuery processedQuery, Schema schema) {
        this.queryNode = queryNode;
        this.processedQuery = processedQuery;
        this.querySchema = new QuerySchema(schema);
        buildQuery();
    }

    public void buildQuery() {
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

        if (!errors.isEmpty() || !querySchema.getErrors().isEmpty()) {
            return;
        }
        if (postAggregations.isEmpty()) {
            query = new Query(projection, filter, aggregation, null, window, duration);
        } else {
            query = new Query(projection, filter, aggregation, postAggregations, window, duration);
        }
    }

    public void doCommon() {
        QueryExtractor queryExtractor = new QueryExtractor(queryNode);

        window = queryExtractor.getWindow();
        duration = queryExtractor.getDuration();
        limit = queryExtractor.getLimit();

        ExpressionNode whereNode = queryNode.getWhere();
        if (whereNode != null) {
            filter = ExpressionVisitor.visit(queryNode.getWhere(), querySchema);
            if (cannotCastToBoolean(filter.getType())) {
                addError(whereNode, "WHERE clause cannot be casted to BOOLEAN: " + whereNode, "Please specify a valid WHERE clause.");
            }
        }
    }

    private static boolean cannotCastToBoolean(Type type) {
        return !Type.isUnknown(type) && !Type.canForceCast(Type.BOOLEAN, type);
    }

    private static boolean isNonPrimitive(Type type) {
        return !Type.isUnknown(type) && !Type.isPrimitive(type);
    }

    public void doOrderBy() {
        if (queryNode.getOrderBy() != null) {
            List<OrderBy.SortItem> sortItems = new ArrayList<>();
            for (SortItemNode sortItemNode : processedQuery.getSortItems()) {
                ExpressionNode orderByNode = sortItemNode.getExpression();
                Expression expression = ExpressionVisitor.visit(orderByNode, querySchema);
                if (isNonPrimitive(expression.getType())) {
                    addError(orderByNode, "ORDER BY contains a non-primitive field: " + orderByNode, "Please specify a primitive field.");
                }
                sortItems.add(new OrderBy.SortItem(expression, sortItemNode.getOrdering().getDirection()));
            }
            postAggregations.add(new OrderBy(sortItems));
        }
    }

    private void addError(ExpressionNode node, String message, String resolution) {
        errors.add(new BulletError(node.getLocation() + message, resolution));
    }

    private void addError(String message, String resolution) {
        errors.add(new BulletError(message, resolution));
    }

    public void doSimpleProjection() {
        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String newName = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            querySchema.addProjectionField(newName, expression);
            querySchema.addSchemaField(newName, type);
            if (processedQuery.hasAlias(node)) {
                querySchema.addAlias(node.getName(), newName);
            }
        }
    }

    private static Optional<List<String>> duplicates(List<String> fields) {
        Map<String, Long> fieldsToCount = fields.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        List<String> duplicates = fieldsToCount.keySet().stream().filter(s -> fieldsToCount.get(s) > 1).collect(Collectors.toList());
        return !duplicates.isEmpty() ? Optional.of(duplicates) : Optional.empty();
    }

    public void doSelect() {
        doSimpleProjection();

        querySchema.nextLayer(true);

        aggregation = new Raw(limit);

        if (queryNode.getOrderBy() != null) {
            OrderByProcessor orderByProcessor = new OrderByProcessor();
            orderByProcessor.process(processedQuery.getOrderByNodes(), querySchema);

            doOrderBy();

            if (!orderByProcessor.getTransientFields().isEmpty()) {
                postAggregations.add(new Culling(orderByProcessor.getTransientFields()));
            }
        }

        // Check duplicate fields at the end because OrderBy can add fields to the projection.
        List<String> fields = querySchema.getProjectionFields().stream().map(Field::getName).collect(Collectors.toList());
        duplicates(fields).ifPresent(duplicates ->
            addError("The following field names are shared: " + duplicates, "Please specify non-overlapping field names.")
        );

        projection = new Projection(querySchema.getProjectionFields(), false);
    }

    public void doSelectAll() {
        aggregation = new Raw(limit);

        doSimpleProjection();

        // Check duplicate fields
        List<String> fields = querySchema.getProjectionFields().stream().map(Field::getName).collect(Collectors.toList());
        duplicates(fields).ifPresent(duplicates ->
            addError("The following field names are shared: " + duplicates, "Please specify non-overlapping field names.")
        );

        boolean requiresCopyFlag = processedQuery.getSelectNodes().stream().anyMatch(node -> !(node instanceof FieldExpressionNode) || processedQuery.hasAlias(node));

        if (requiresCopyFlag) {
            projection = new Projection(querySchema.getProjectionFields(), true);
        } else {
            projection = new Projection();
        }

        querySchema.nextLayer(requiresCopyFlag);

        doOrderBy();

        // Renamed fields that are not in the schema are transient
        if (requiresCopyFlag) {
            Set<String> transientFields = new HashSet<>(querySchema.getCurrentAliasMapping().keySet());
            transientFields.removeAll(querySchema.getCurrentSchema().keySet());
            if (!transientFields.isEmpty()) {
                postAggregations.add(new Culling(transientFields));
            }
        }
    }

    public void doSelectDistinct() {
        Map<String, String> fields = new HashMap<>();
        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String newName = processedQuery.getAliasOrName(node);
            String name = node.getName();
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                querySchema.addAlias(name, newName);
            }
            querySchema.addProjectionField(name, expression);
            querySchema.addSchemaField(newName, type);

            fields.put(name, newName);

            if (isNonPrimitive(type)) {
                addError(node, "The SELECT DISTINCT field " + node + " is non-primitive. Type given: " + type, "Please specify primitive fields only for SELECT DISTINCT.");
            }
        }
/*
        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            if (isNonPrimitive(expression.getType())) {
                addError(node, "The SELECT DISTINCT field " + node + " is non-primitive. Type given: " + type, "Please specify primitive fields only for SELECT DISTINCT.");
            }
        }
*/
        // Check duplicate fields
        List<String> schemaFields = querySchema.getProjectionFields().stream().map(Field::getName).collect(Collectors.toList());
        duplicates(schemaFields).ifPresent(duplicates ->
            addError("The following field names are shared: " + duplicates, "Please specify non-overlapping field names.")
        );

        boolean requiresNoCopyFlag = processedQuery.getSelectNodes().stream().anyMatch(node -> !(node instanceof FieldExpressionNode));

        if (requiresNoCopyFlag) {
            projection = new Projection(querySchema.getProjectionFields(), false);
        } else {
            projection = new Projection();
        }

        querySchema.nextLayer(true);

        aggregation = new GroupBy(limit, fields, Collections.emptySet());

        doOrderBy();
    }


    public void doComputation() {
        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            if (querySchema.contains(node)) {
                continue;
            }
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String newName = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                querySchema.addAlias(node.getName(), newName);
            }
            querySchema.addComputationField(newName, expression);
            querySchema.addSchemaField(newName, type);
        }
        List<Field> fields = querySchema.getComputationFields();
        List<String> names = fields.stream().map(Field::getName).collect(Collectors.toList());
        duplicates(names).ifPresent(duplicates ->
            addError("The following field names/aliases are shared: " + duplicates, "Please specify non-overlapping field names and aliases.")
        );
        if (!fields.isEmpty()) {
            postAggregations.add(new Computation(fields));
        }
    }


    public void doGroup() {
        Map<String, String> fields = new HashMap<>();

        boolean requiresNoCopyFlag = false;

        List<String> schemaFields = new ArrayList<>();

        for (ExpressionNode node : processedQuery.getGroupByNodes()) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String newName = processedQuery.getAliasOrName(node);
            String name = node.getName();
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                querySchema.addAlias(name, newName);
            }
            querySchema.addProjectionField(name, expression);
            querySchema.addSchemaField(newName, type);

            fields.put(name, newName);

            schemaFields.add(newName);

            if (!(node instanceof FieldExpressionNode)) {
                requiresNoCopyFlag = true;
            }

            if (isNonPrimitive(type)) {
                addError(node, "The GROUP BY field " + node + " is non-primitive. Type given: " + type, "Please specify primitive fields only for GROUP BY.");
            }
        }

        Set<GroupOperation> operations = new HashSet<>();

        for (GroupOperationNode node : processedQuery.getGroupOpNodes()) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String name = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            // Mapping for the aggregate
            if (processedQuery.hasAlias(node)) {
                querySchema.addAlias(node.getName(), name);
            }
            querySchema.addSchemaField(name, type);

            schemaFields.add(name);

            ExpressionNode expressionNode = node.getExpression();
            if (expressionNode != null) {
                querySchema.addProjectionField(expressionNode.getName(), ExpressionVisitor.visit(expressionNode, querySchema));
                operations.add(new GroupOperation(node.getOp(), expressionNode.getName(), name));
                if (!(expressionNode instanceof FieldExpressionNode)) {
                    requiresNoCopyFlag = true;
                }
            } else {
                operations.add(new GroupOperation(node.getOp(), null, name));
            }
        }

        if (requiresNoCopyFlag) {
            projection = new Projection(querySchema.getProjectionFields(), false);
        } else {
            projection = new Projection();
        }

        // Check duplicate fields
        List<String> projectionFields = querySchema.getProjectionFields().stream().map(Field::getName).collect(Collectors.toList());
        duplicates(projectionFields).ifPresent(duplicates ->
            addError("The following field names are shared: " + duplicates, "Please specify non-overlapping field names.")
        );

        duplicates(schemaFields).ifPresent(duplicates ->
            addError("The following field names/aliases are shared: " + duplicates, "Please specify non-overlapping field names and aliases.")
        );

        if (!fields.isEmpty()) {
            aggregation = new GroupBy(limit, fields, operations);
        } else {
            aggregation = new GroupAll(operations);
        }

        querySchema.nextLayer(true);

        ExpressionNode having = queryNode.getHaving();
        if (having != null) {
            Expression expression = ExpressionVisitor.visit(having, querySchema);
            Type type = expression.getType();
            if (cannotCastToBoolean(type)) {
                addError(having, "HAVING clause cannot be casted to BOOLEAN: " + having, "Please specify a valid HAVING clause.");
            }
            postAggregations.add(new Having(expression));
        }

        doComputation();

        querySchema.nextLayer(false);

        doOrderBy();

        // transient
        // every field that's not a select field
        Set<String> transientFields = new HashSet<>(querySchema.getCurrentSchema().keySet());
        processedQuery.getSelectNodes().stream().map(processedQuery::getAliasOrName).forEach(transientFields::remove);
        if (!transientFields.isEmpty()) {
            postAggregations.add(new Culling(transientFields));
        }
    }

    public void doCountDistinct() {
        CountDistinctNode countDistinctNode = processedQuery.getCountDistinct();
        List<ExpressionNode> countDistinctExpressions = countDistinctNode.getExpressions();

        List<String> fields = new ArrayList<>();

        for (ExpressionNode node : countDistinctExpressions) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String name = node.getName();
            querySchema.addProjectionField(name, expression);
            fields.add(name);
        }

        //
        boolean requiresNoCopyFlag = countDistinctExpressions.stream().anyMatch(node -> !(node instanceof FieldExpressionNode));

        if (requiresNoCopyFlag) {
            projection = new Projection(querySchema.getProjectionFields(), false);
        } else {
            projection = new Projection();
        }


        Expression countDistinctExpression = ExpressionVisitor.visit(countDistinctNode, querySchema);
        String countDistinctName = processedQuery.getAliasOrName(countDistinctNode);
        if (processedQuery.hasAlias(countDistinctNode)) {
            querySchema.addAlias(countDistinctNode.getName(), countDistinctName);
        }
        querySchema.addSchemaField(countDistinctName, countDistinctExpression.getType());

        querySchema.nextLayer(true);

        aggregation = new CountDistinct(fields, countDistinctName);

        doComputation();

        querySchema.nextLayer(false);

        // possible transient if count(distinct .......) was never selected
        Set<String> transientFields = new HashSet<>(querySchema.getCurrentSchema().keySet());
        processedQuery.getSelectNodes().stream().map(processedQuery::getAliasOrName).forEach(transientFields::remove);
        if (!transientFields.isEmpty()) {
            postAggregations.add(new Culling(transientFields));
        }
    }

    public void doDistribution() {
        DistributionNode distributionNode = processedQuery.getDistribution();

        ExpressionNode distributionExpressionNode = distributionNode.getExpression();
        Expression distributionExpression = ExpressionVisitor.visit(distributionExpressionNode, querySchema);
        String distributionExpressionName = distributionExpressionNode.getName();
        querySchema.addProjectionField(distributionExpressionName, distributionExpression);

        boolean requiresNoCopyFlag = !(distributionExpressionNode instanceof FieldExpressionNode);

        if (requiresNoCopyFlag) {
            projection = new Projection(querySchema.getProjectionFields(), false);
        } else {
            projection = new Projection();
        }

        ExpressionVisitor.visit(distributionNode, querySchema);

        querySchema.setDistributionFields(distributionNode.getType());

        querySchema.nextLayer(true);

        aggregation = distributionNode.getAggregation(limit);

        doComputation();

        querySchema.nextLayer(false);

        doOrderBy();
    }

    public void doTopK() {
        TopKNode topKNode = processedQuery.getTopK();
        List<ExpressionNode> topKExpressions = topKNode.getExpressions();
        Map<String, String> fields = new HashMap<>();

        for (ExpressionNode node : topKExpressions) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            Type type = expression.getType();
            String name = node.getName();
            String aliasOrName = processedQuery.getAliasOrName(node);
            if (processedQuery.hasAlias(node)) {
                querySchema.addAlias(name, aliasOrName);
            }
            querySchema.addProjectionField(name, expression);
            querySchema.addSchemaField(aliasOrName, type);
            fields.put(name, aliasOrName);
        }

        boolean requiresNoCopyFlag = topKExpressions.stream().anyMatch(node -> !(node instanceof FieldExpressionNode));

        if (requiresNoCopyFlag) {
            projection = new Projection(querySchema.getProjectionFields(), false);
        } else {
            projection = new Projection();
        }

        ExpressionVisitor.visit(topKNode, querySchema);
        String topKAlias = processedQuery.getAlias(topKNode);
        querySchema.addSchemaField(topKAlias, Type.LONG);

        querySchema.nextLayer(true);

        aggregation = new TopK(fields, topKNode.getSize(), topKNode.getThreshold(), topKAlias);

        doComputation();
    }

    public void doSpecialK() {
        Map<String, String> fields = new HashMap<>();

        for (ExpressionNode node : processedQuery.getGroupByNodes()) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            Type type = expression.getType();
            String name = node.getName();
            String aliasOrName = processedQuery.getAliasOrName(node);
            if (processedQuery.hasAlias(node)) {
                querySchema.addAlias(name, aliasOrName);
            }
            querySchema.addProjectionField(name, expression);
            querySchema.addSchemaField(aliasOrName, type);
            fields.put(name, aliasOrName);

            if (isNonPrimitive(type)) {
                addError(node, "The GROUP BY field " + node + " is non-primitive. Type given: " + type, "Please specify primitive fields only for GROUP BY.");
            }
        }

        boolean requiresNoCopyFlag = processedQuery.getGroupByNodes().stream().anyMatch(node -> !(node instanceof FieldExpressionNode));

        if (requiresNoCopyFlag) {
            projection = new Projection(querySchema.getProjectionFields(), false);
        } else {
            projection = new Projection();
        }

        ExpressionNode countNode = processedQuery.getGroupOpNodes().iterator().next();
        ExpressionVisitor.visit(countNode, querySchema);
        String countAliasOrName = processedQuery.getAliasOrName(countNode);
        if (processedQuery.hasAlias(countNode)) {
            querySchema.addAlias(countNode.getName(), countAliasOrName);
        }
        querySchema.addSchemaField(countAliasOrName, Type.LONG);

        querySchema.nextLayer(true);

        Long threshold = null;
        if (queryNode.getHaving() != null) {
            threshold = ((Number) ((LiteralNode) ((BinaryExpressionNode) queryNode.getHaving()).getRight()).getValue()).longValue();
        }
        aggregation = new TopK(fields, limit, threshold, countAliasOrName);

        doComputation();
    }

    public List<BulletError> getErrors() {
        List<BulletError> bulletErrors = new ArrayList<>();
        bulletErrors.addAll(errors);
        bulletErrors.addAll(querySchema.getErrors());
        return bulletErrors;
    }

    public boolean hasErrors() {
        return !errors.isEmpty() || !querySchema.getErrors().isEmpty();
    }
}
