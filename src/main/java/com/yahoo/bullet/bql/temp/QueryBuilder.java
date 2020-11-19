package com.yahoo.bullet.bql.temp;

import com.yahoo.bullet.bql.extractor.DurationExtractor;
import com.yahoo.bullet.bql.extractor.QueryExtractor;
import com.yahoo.bullet.bql.extractor.WindowExtractor;
import com.yahoo.bullet.bql.query.ExpressionVisitor;
import com.yahoo.bullet.bql.query.OrderByProcessor;
import com.yahoo.bullet.bql.query.ProcessedQuery;
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
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.query.postaggregations.Having;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.query.postaggregations.PostAggregation;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private Set<ProcessedQuery.QueryError> queryErrors = EnumSet.noneOf(ProcessedQuery.QueryError.class);

    private List<BulletError> errors = new ArrayList<>();

    private Query query;

    private List<PostAggregation> postAggregations = new ArrayList<>();

    public QueryBuilder(QueryNode queryNode, ProcessedQuery processedQuery, Schema schema) {
        this.queryNode = queryNode;
        this.processedQuery = processedQuery;
        this.querySchema = new QuerySchema(schema);
    }

    public Query buildQuery() {
        doSetup();

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
                doGroup();
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
            case SPECIAL_K:
                doSpecialK();
                break;
        }

        if (!errors.isEmpty() || !queryErrors.isEmpty() || !querySchema.getTypeErrors().isEmpty()) {
            return null;
        }

        if (postAggregations.isEmpty()) {
            query = new Query(projection, filter, aggregation, null, window, duration);
        } else {
            query = new Query(projection, filter, aggregation, postAggregations, window, duration);
        }

        return query;
    }

    public void doSetup() {
        // common
        window = WindowExtractor.extractWindow(queryNode.getWindow());
        duration = DurationExtractor.extractDuration(queryNode.getStream());
        limit = QueryExtractor.extractLimit(queryNode);

        ExpressionNode whereNode = queryNode.getWhere();
        if (whereNode != null) {
            filter = ExpressionVisitor.visit(queryNode.getWhere(), querySchema);
            if (!Type.isUnknown(filter.getType()) && !Type.canForceCast(Type.BOOLEAN, filter.getType())) {
                errors.add(new BulletError(whereNode.getLocation() + "WHERE clause cannot be casted to BOOLEAN: " + whereNode,
                                           "Please specify a valid WHERE clause."));
            }
        }

    }

    private static Optional<List<String>> duplicates(List<String> fields) {
        Map<String, Long> fieldsToCount = fields.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        List<String> duplicates = fieldsToCount.keySet().stream().filter(s -> fieldsToCount.get(s) > 1).collect(Collectors.toList());
        return !duplicates.isEmpty() ? Optional.of(duplicates) : Optional.empty();
    }

    public void doSelect() {
        // This is select only. not select all. we only have select fields and order by to worry about after
        // the select nodes

        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String name = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                querySchema.addAliasMapping(name, type);
            }
            querySchema.addProjectionField(name, node, expression);
        }

        querySchema.nextLevel(true);

        aggregation = new Raw(limit);

        if (queryNode.getOrderBy() != null) {
            List<OrderBy.SortItem> sortItems = new ArrayList<>();

            OrderByProcessor orderByProcessor = new OrderByProcessor();
            orderByProcessor.process(processedQuery.getOrderByNodes(), querySchema);

            for (SortItemNode sortItemNode : processedQuery.getSortItemNodes()) {
                ExpressionNode orderByNode = sortItemNode.getExpression();
                Expression expression = ExpressionVisitor.visit(orderByNode, querySchema);
                Type type = expression.getType();
                // Validate prim type here.
                if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                    errors.add(new BulletError(orderByNode.getLocation() + "ORDER BY contains a non-primitive field: " + orderByNode, "Please specify a primitive field."));
                }
                sortItems.add(new OrderBy.SortItem(expression, sortItemNode.getOrdering().getDirection()));
            }

            postAggregations.add(new OrderBy(sortItems));

            if (!orderByProcessor.getTransientFields().isEmpty()) {
                postAggregations.add(new Culling(orderByProcessor.getTransientFields()));
            }
        }

        // Check duplicate fields at the end because OrderBy can add fields to the projection.
        List<String> fields = querySchema.getFields().stream().map(Field::getName).collect(Collectors.toList());
        duplicates(fields).ifPresent(duplicates -> {
            errors.add(new BulletError("The following field names are shared: " + duplicates,
                                       "Please specify non-overlapping field names."));
        });

        projection = new Projection(querySchema.getProjectionFields(), false);
    }

    public void doSelectAll() {
        aggregation = new Raw(limit);

        // QueryExtractor.extractSelect/Nodes() ?

        // select all

        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String name = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                querySchema.addAliasMapping(name, type);
            }
            querySchema.addProjectionField(name, node, expression);
        }

        // Check duplicate fields
        List<String> fields = querySchema.getFields().stream().map(Field::getName).collect(Collectors.toList());
        duplicates(fields).ifPresent(duplicates ->
            errors.add(new BulletError("The following field names are shared: " + duplicates,
                                       "Please specify non-overlapping field names."))
        );

        // crap subfieldnode is a fieldnode
        boolean requiresCopyFlag = processedQuery.getSelectNodes().stream().anyMatch(node -> !(node instanceof FieldExpressionNode) || processedQuery.hasAlias(node));

        // ^ actually we can check this part first to decide if we do the thing above it or not... heh
        // .. but even if it's passthrough, still need to check all fieldexpnodes. so.
        // if passthrough, dont need to add the nodes to a schema though

        if (requiresCopyFlag) {
            projection = new Projection(querySchema.getProjectionFields(), true);
        } else {
            projection = new Projection();
        }

        querySchema.nextLevel(false);


        if (queryNode.getOrderBy() != null) {
            List<OrderBy.SortItem> sortItems = new ArrayList<>();
            for (SortItemNode sortItemNode : processedQuery.getSortItemNodes()) {
                ExpressionNode orderByNode = sortItemNode.getExpression();
                Expression expression = ExpressionVisitor.visit(orderByNode, querySchema);
                Type type = expression.getType();
                // Validate prim type here.
                if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                    errors.add(new BulletError(orderByNode.getLocation() + "ORDER BY contains a non-primitive field: " + orderByNode, "Please specify a primitive field."));
                }
                // else?
                sortItems.add(new OrderBy.SortItem(expression, sortItemNode.getOrdering().getDirection()));
            }
            postAggregations.add(new OrderBy(sortItems));
        }


        // no computation cuz that's taken care of by copy projection


        // there is a transient for fields that you rename...............................
        if (requiresCopyFlag) {
            Set<String> remappedFieldNames = querySchema.getCurrentFieldMapping().keySet().stream()
                                                                                          .filter(node -> node instanceof FieldExpressionNode)
                                                                                          .map(node -> ((FieldExpressionNode) node).getField().getValue())
                                                                                          .collect(Collectors.toCollection(HashSet::new));
            Set<String> mappedFieldNames = Stream.concat(querySchema.getCurrentAliasMapping().values().stream(),
                                                         querySchema.getCurrentFieldMapping().values().stream())
                                                 .map(FieldExpression::getField)
                                                 .collect(Collectors.toSet());
            remappedFieldNames.removeAll(mappedFieldNames);
            if (!remappedFieldNames.isEmpty()) {
                postAggregations.add(new Culling(remappedFieldNames));
            }
        }
    }

    public void doSelectDistinct() {

        // select distinct

        Map<String, String> fields = new HashMap<>();

        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String aliasOrName = processedQuery.getAliasOrName(node);
            String name = node.getName();
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                querySchema.addAliasMapping(aliasOrName, type);
            }
            //querySchema.addProjectionField(name, node, type);
            // uhhhhhhh
            querySchema.addFieldMapping(aliasOrName, node, type);
            // hmmm
            querySchema.addProjectionField(name, expression);
            // ^ goood for potential passthrough. i guess that's the reason

            fields.put(name, aliasOrName);

            // Check primitive type
            if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                errors.add(new BulletError(node.getLocation() + "The SELECT DISTINCT field " + node + " is non-primitive. Type given: " + type,
                                           "Please specify primitive fields only for SELECT DISTINCT."));
            }
        }

        // Check duplicate fields
        List<String> schemaFields = querySchema.getFields().stream().map(Field::getName).collect(Collectors.toList());
        duplicates(schemaFields).ifPresent(duplicates ->
            errors.add(new BulletError("The following field names are shared: " + duplicates,
                                       "Please specify non-overlapping field names."))
        );

        boolean requiresNoCopyFlag = processedQuery.getSelectNodes().stream().anyMatch(node -> !(node instanceof FieldExpressionNode));

        if (requiresNoCopyFlag) {
            projection = new Projection(querySchema.getProjectionFields(), false);
        } else {
            projection = new Projection();
        }

        querySchema.nextLevel(true);

        /*
        aggregation

        group_by.

        all select nodes -> get field if there or just name. getaliasorname

        */

        aggregation = new GroupBy(limit, fields, Collections.emptySet());


        // no having

        // no computations or transient

        /*
        order by
        */
        if (queryNode.getOrderBy() != null) {
            List<OrderBy.SortItem> sortItems = new ArrayList<>();
            for (SortItemNode sortItemNode : processedQuery.getSortItemNodes()) {
                ExpressionNode orderByNode = sortItemNode.getExpression();
                Expression expression = ExpressionVisitor.visit(orderByNode, querySchema);
                Type type = expression.getType();
                // Validate prim type here.
                if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                    errors.add(new BulletError(orderByNode.getLocation() + "ORDER BY contains a non-primitive field: " + orderByNode, "Please specify a primitive field."));
                }
                // else?
                sortItems.add(new OrderBy.SortItem(expression, sortItemNode.getOrdering().getDirection()));
            }
            postAggregations.add(new OrderBy(sortItems));
        }
    }

    public void doGroup() {
        Map<String, String> fields = new HashMap<>();

        boolean requiresNoCopyFlag = false;

        List<String> schemaFields = new ArrayList<>();

        for (ExpressionNode node : processedQuery.getGroupByNodes()) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String aliasOrName = processedQuery.getAliasOrName(node);
            String name = node.getName();
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                querySchema.addAliasMapping(aliasOrName, type);
            }
            querySchema.addFieldMapping(aliasOrName, node, type);
            querySchema.addProjectionField(name, expression);

            fields.put(name, aliasOrName);

            schemaFields.add(aliasOrName);

            if (!(node instanceof FieldExpressionNode)) {
                requiresNoCopyFlag = true;
            }

            // Check primitive type
            if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                errors.add(new BulletError(node.getLocation() + "The GROUP BY field " + node + " is non-primitive. Type given: " + type,
                                           "Please specify primitive fields only for GROUP BY."));
            }
        }

        Set<GroupOperation> operations = new HashSet<>();

        for (GroupOperationNode node : processedQuery.getGroupOpNodes()) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String name = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            // Mapping for the aggregate
            if (processedQuery.hasAlias(node)) {
                querySchema.addAliasMapping(name, type);
            }
            querySchema.addFieldMapping(name, node, type);

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

        // need to check the group by and inner group op ........................ above? requiresnoCopyFlag = true;
        //boolean requiresNoCopyFlag = processedQuery.getSelectNodes().stream().anyMatch(node -> !(node instanceof FieldExpressionNode));

        if (requiresNoCopyFlag) {
            projection = new Projection(querySchema.getProjectionFields(), false);
        } else {
            projection = new Projection();
        }

        /*
        aggregation / group by nodes

        post agg mapping

        validate on second layer of schema.

        apply field
         */

        // Check duplicate fields
        List<String> projectionFields = querySchema.getFields().stream().map(Field::getName).collect(Collectors.toList());
        duplicates(projectionFields).ifPresent(duplicates ->
                errors.add(new BulletError("The following field names are shared: " + duplicates,
                                           "Please specify non-overlapping field names."))
        );

        duplicates(schemaFields).ifPresent(duplicates ->
                errors.add(new BulletError("The following field names/aliases are shared: " + duplicates,
                                           "Please specify non-overlapping field names and aliases."))
        );

        if (!fields.isEmpty()) {
            aggregation = new GroupBy(limit, fields, operations);
        } else {
            aggregation = new GroupAll(operations);
        }

        querySchema.nextLevel(true);

        // having

        ExpressionNode having = queryNode.getHaving();
        if (having != null) {
            Expression expression = ExpressionVisitor.visit(having, querySchema);
            Type type = expression.getType();
            if (!Type.isUnknown(type) && !Type.canForceCast(Type.BOOLEAN, type)) {
                errors.add(new BulletError(having.getLocation() + "HAVING clause cannot be casted to BOOLEAN: " + having,
                                           "Please specify a valid HAVING clause."));
            }
            postAggregations.add(new Having(expression));
        }

        // computation
        // everything that's not already in the schema would work probably

        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            if (querySchema.getAliasOrField(node) != null) {
                continue;
            }
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String name = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                querySchema.addAliasMapping(name, type);
            }
            //querySchema.addFieldMapping(name, node, type);
            querySchema.addComputationField(name, node, expression);
        }

        List<Field> computationFields = querySchema.getComputationFields();
        List<String> computationNames = computationFields.stream().map(Field::getName).collect(Collectors.toList());
        duplicates(computationNames).ifPresent(duplicates ->
                errors.add(new BulletError("The following field names/aliases are shared: " + duplicates,
                                           "Please specify non-overlapping field names and aliases."))
        );

        if (!computationFields.isEmpty()) {
            postAggregations.add(new Computation(computationFields));
        }

        querySchema.nextLevel(false);

        // order by
        if (queryNode.getOrderBy() != null) {
            List<OrderBy.SortItem> sortItems = new ArrayList<>();
            for (SortItemNode sortItemNode : processedQuery.getSortItemNodes()) {
                ExpressionNode orderByNode = sortItemNode.getExpression();
                Expression expression = ExpressionVisitor.visit(orderByNode, querySchema);
                Type type = expression.getType();
                // Validate prim type here.
                if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                    errors.add(new BulletError(orderByNode.getLocation() + "ORDER BY contains a non-primitive field: " + orderByNode, "Please specify a primitive field."));
                }
                // else?
                sortItems.add(new OrderBy.SortItem(expression, sortItemNode.getOrdering().getDirection()));
            }
            postAggregations.add(new OrderBy(sortItems));
        }

        // transient
        // every field that's not a select field.........???
        Set<String> mappedFieldNames = Stream.concat(querySchema.getCurrentAliasMapping().values().stream(),
                                                     querySchema.getCurrentFieldMapping().values().stream())
                                             .map(FieldExpression::getField)
                                             .collect(Collectors.toCollection(HashSet::new));
        processedQuery.getSelectNodes().stream().map(processedQuery::getAliasOrName).forEach(mappedFieldNames::remove);

        if (!mappedFieldNames.isEmpty()) {
            postAggregations.add(new Culling(mappedFieldNames));
        }
    }

    public void doCountDistinct() {
        /*
        select count(distinct of some expression here, could be multiple, damn) + could be in an expression, and have other random shit where
         */
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
            querySchema.addAliasMapping(countDistinctName, countDistinctExpression.getType());
        }
        querySchema.addFieldMapping(countDistinctName, countDistinctNode, countDistinctExpression.getType());

        querySchema.nextLevel(true);

        aggregation = new CountDistinct(fields, countDistinctName);


        // no having

        // computation
        // everything that's not count distinct i guess
        // TODO ok that's actually a bug ^ needs to check querySchema.getAliasOrField

        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            //if (querySchema.getAliasOrField(node) != null) {
            if (node.equals(countDistinctNode)) {
                continue;
            }
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String name = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                querySchema.addAliasMapping(name, type);
            }
            querySchema.addComputationField(name, node, expression);
        }

        List<Field> computationFields = querySchema.getComputationFields();
        List<String> computationNames = computationFields.stream().map(Field::getName).collect(Collectors.toList());
        duplicates(computationNames).ifPresent(duplicates ->
                errors.add(new BulletError("The following field names/aliases are shared: " + duplicates,
                                           "Please specify non-overlapping field names and aliases."))
        );

        if (!computationFields.isEmpty()) {
            postAggregations.add(new Computation(computationFields));
        }

        querySchema.nextLevel(false);

        // order by
        if (queryNode.getOrderBy() != null) {
            List<OrderBy.SortItem> sortItems = new ArrayList<>();
            for (SortItemNode sortItemNode : processedQuery.getSortItemNodes()) {
                ExpressionNode orderByNode = sortItemNode.getExpression();
                Expression expression = ExpressionVisitor.visit(orderByNode, querySchema);
                Type type = expression.getType();
                // Validate prim type here.
                if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                    errors.add(new BulletError(orderByNode.getLocation() + "ORDER BY contains a non-primitive field: " + orderByNode, "Please specify a primitive field."));
                }
                // else?
                sortItems.add(new OrderBy.SortItem(expression, sortItemNode.getOrdering().getDirection()));
            }
            postAggregations.add(new OrderBy(sortItems));
        }


        // possible transient if count(distinct .......) was never selected

        // every field that's not a select field.........???
        //Set<String> mappedFieldNames = Stream.concat(querySchema.getCurrentAliasMapping().values().stream(),
        //                                             querySchema.getCurrentFieldMapping().values().stream())
        //                                     .map(FieldExpression::getField)
        //                                     .collect(Collectors.toCollection(HashSet::new));
        //processedQuery.getSelectNodes().stream().map(processedQuery::getAliasOrName).forEach(mappedFieldNames::remove);

        Set<String> mappedFieldNames = querySchema.getCurrentFieldMapping().values().stream()
                                                                                    .map(FieldExpression::getField)
                                                                                    .collect(Collectors.toCollection(HashSet::new));
        processedQuery.getSelectNodes().stream().map(processedQuery::getAliasOrName).forEach(mappedFieldNames::remove);

        // really only have to check if mappedFieldNames has countDistinctNode.getName() / COUNT(DISTINCT ......) in it or not

        if (!mappedFieldNames.isEmpty()) {
            postAggregations.add(new Culling(mappedFieldNames));
        }

        //if (!mappedFieldNames.contains(countDistinctName)) {
        //    Set<String> transientFields = new HashSet<>();
        //    transientFields.add(countDistinctName);
        //    postAggregations.add(new Culling(transientFields));
        //}
    }

    public void doDistribution() {

        /*

        select distribution(of this possible expression), only computations here afterward

         */

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

        querySchema.addDistributionFields(distributionNode.getType());

        querySchema.nextLevel(true);

        aggregation = distributionNode.getAggregation(limit);


        // no having

        // computation
        // everything that's not the distribution node

        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            //if (querySchema.get(node) != null) {
            if (node.equals(distributionNode)) {
                continue;
            }
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String name = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                querySchema.addAliasMapping(name, type);
            }
            querySchema.addComputationField(name, node, expression);
        }

        List<Field> computationFields = querySchema.getComputationFields();
        List<String> computationNames = computationFields.stream().map(Field::getName).collect(Collectors.toList());
        duplicates(computationNames).ifPresent(duplicates ->
                errors.add(new BulletError("The following field names/aliases are shared: " + duplicates,
                                           "Please specify non-overlapping field names and aliases."))
        );

        if (!computationFields.isEmpty()) {
            postAggregations.add(new Computation(computationFields));
        }

        querySchema.nextLevel(false);

        // order by
        if (queryNode.getOrderBy() != null) {
            List<OrderBy.SortItem> sortItems = new ArrayList<>();
            for (SortItemNode sortItemNode : processedQuery.getSortItemNodes()) {
                ExpressionNode orderByNode = sortItemNode.getExpression();
                Expression expression = ExpressionVisitor.visit(orderByNode, querySchema);
                Type type = expression.getType();
                // Validate prim type here.
                if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                    errors.add(new BulletError(orderByNode.getLocation() + "ORDER BY contains a non-primitive field: " + orderByNode, "Please specify a primitive field."));
                }
                // else?
                sortItems.add(new OrderBy.SortItem(expression, sortItemNode.getOrdering().getDirection()));
            }
            postAggregations.add(new OrderBy(sortItems));
        }
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
            // Might be wrong
            if (processedQuery.hasAlias(node)) {
                querySchema.addAliasMapping(aliasOrName, type);
            }
            querySchema.addProjectionField(name, expression);
            querySchema.addFieldMapping(aliasOrName, node, type);
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
        querySchema.addAliasMapping(topKAlias, Type.LONG);

        querySchema.nextLevel(true);

        aggregation = new TopK(fields, topKNode.getSize(), topKNode.getThreshold(), topKAlias);


        // no having

        // computation
        // everything that's not the top k node or its expressions

        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            //if (querySchema.get(node) != null) {
            if (node.equals(topKNode) || querySchema.getAliasOrField(node) != null) {
                continue;
            }
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String name = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                querySchema.addAliasMapping(name, type);
            }
            querySchema.addComputationField(name, node, expression);
        }

        List<Field> computationFields = querySchema.getComputationFields();
        List<String> computationNames = computationFields.stream().map(Field::getName).collect(Collectors.toList());
        duplicates(computationNames).ifPresent(duplicates ->
                errors.add(new BulletError("The following field names/aliases are shared: " + duplicates,
                                           "Please specify non-overlapping field names and aliases."))
        );

        if (!computationFields.isEmpty()) {
            postAggregations.add(new Computation(computationFields));
        }

        querySchema.nextLevel(false);
    }

    public void doSpecialK() {
        /*
        ExpressionNode countNode = processedQuery.getGroupOpNodes().iterator().next();
        Map<String, String> fields = toAliasedFields(processedQuery, processedQuery.getGroupByNodes());
        Long threshold = null;
        if (processedQuery.getHavingNode() != null) {
            threshold = ((Number) ((LiteralNode) ((BinaryExpressionNode) processedQuery.getHavingNode()).getRight()).getValue()).longValue();
        }
        addSimplePostAggregationMapping(processedQuery, processedQuery.getGroupByNodes());
        addComplexPostAggregationMapping(processedQuery, processedQuery.getGroupByNodes());
        addPostAggregationMapping(processedQuery, countNode);
        return new TopK(fields, processedQuery.getLimit(), threshold, processedQuery.getAliasOrName(countNode));
         */


        Map<String, String> fields = new HashMap<>();

        for (ExpressionNode node : processedQuery.getGroupByNodes()) {
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            Type type = expression.getType();
            String name = node.getName();
            String aliasOrName = processedQuery.getAliasOrName(node);
            // Might be wrong
            if (processedQuery.hasAlias(node)) {
                querySchema.addAliasMapping(aliasOrName, type);
            }
            querySchema.addProjectionField(name, expression);
            querySchema.addFieldMapping(aliasOrName, node, type);
            fields.put(name, aliasOrName);

            // Check primitive type
            if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                errors.add(new BulletError(node.getLocation() + "The GROUP BY field " + node + " is non-primitive. Type given: " + type,
                                           "Please specify primitive fields only for GROUP BY."));
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
        //querySchema.addAliasMapping(countAliasOrName, Type.LONG);
        querySchema.addFieldMapping(countAliasOrName, countNode, Type.LONG);

        querySchema.nextLevel(true);

        Long threshold = null;
        if (queryNode.getHaving() != null) {
            threshold = ((Number) ((LiteralNode) ((BinaryExpressionNode) queryNode.getHaving()).getRight()).getValue()).longValue();
        }
        aggregation = new TopK(fields, limit, threshold, countAliasOrName);

        // no having

        // computation
        //Set<ExpressionNode> expressions = new HashSet<>(processedQuery.getSelectNodes());
        //expressions.removeAll(processedQuery.getGroupByNodes());
        //expressions.removeAll(processedQuery.getGroupOpNodes());

        for (ExpressionNode node : processedQuery.getSelectNodes()) {
            //if (querySchema.get(node) != null) {
            if (querySchema.getAliasOrField(node) != null) {
                continue;
            }
            Expression expression = ExpressionVisitor.visit(node, querySchema);
            String name = processedQuery.getAliasOrName(node);
            Type type = expression.getType();
            if (processedQuery.hasAlias(node)) {
                querySchema.addAliasMapping(name, type);
            }
            querySchema.addComputationField(name, node, expression);
        }

        List<Field> computationFields = querySchema.getComputationFields();
        List<String> computationNames = computationFields.stream().map(Field::getName).collect(Collectors.toList());
        duplicates(computationNames).ifPresent(duplicates ->
                errors.add(new BulletError("The following field names/aliases are shared: " + duplicates,
                                           "Please specify non-overlapping field names and aliases."))
        );

        if (!computationFields.isEmpty()) {
            postAggregations.add(new Computation(computationFields));
        }

        querySchema.nextLevel(false);
    }

    public List<BulletError> getErrors() {
        List<BulletError> bulletErrors = new ArrayList<>();
        bulletErrors.addAll(errors);
        bulletErrors.addAll(querySchema.getTypeErrors());
        queryErrors.stream().map(ProcessedQuery.QueryError::getError).forEach(bulletErrors::add);
        return bulletErrors;
    }
}
