package com.yahoo.bullet.bql.temp;

import com.yahoo.bullet.bql.extractor.DurationExtractor;
import com.yahoo.bullet.bql.extractor.QueryExtractor;
import com.yahoo.bullet.bql.extractor.WindowExtractor;
import com.yahoo.bullet.bql.query.ExpressionProcessor;
import com.yahoo.bullet.bql.query.ExpressionValidator;
import com.yahoo.bullet.bql.query.ExpressionVisitor;
import com.yahoo.bullet.bql.query.OrderByProcessor;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.FieldExpressionNode;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.aggregations.GroupBy;
import com.yahoo.bullet.query.aggregations.Raw;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.query.postaggregations.PostAggregation;
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryBuilder {
    private ProcessedQuery processedQuery;

    private QueryNode queryNode;

    private Projection projection;
    private Expression filter;
    private Aggregation aggregation;

    //private Expression having;
    //private Expression computation;

    private Window window;

    private Long duration;

    private Integer limit;

    //private Map<ExpressionNode, Expression> preAggregationMapping = new HashMap<>();
    //private Map<ExpressionNode, Expression> postAggregationMapping = new HashMap<>();


    private QuerySchema querySchema;

    private Set<ProcessedQuery.QueryError> queryErrors = EnumSet.noneOf(ProcessedQuery.QueryError.class);

    private List<BulletError> errors = new ArrayList<>();



    private Query query;

    private List<PostAggregation> postAggregations = new ArrayList<>();



    public QueryBuilder(ProcessedQuery processedQuery) {
        this.processedQuery = processedQuery;
    }

    public static Query buildQuery(ProcessedQuery processedQuery) {
        return new QueryBuilder(processedQuery).buildQuery();
    }

    public Query buildQuery() {
        doSetup();

        switch (processedQuery.getQueryType()) {
            case SELECT:
                doSelect();
                break;
            case SELECT_ALL:

            case SELECT_DISTINCT:

            case GROUP:

            case COUNT_DISTINCT:

            case DISTRIBUTION:

            case TOP_K:

            case SPECIAL_K:

        }

        if (a bunch of errors) {
            return some sort of errors;
        }

        build a query and return it????;

        return null;
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
            querySchema.addProjectionField(name, node, type);
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

        projection = new Projection(querySchema.getFields(), false);
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
            querySchema.addProjectionField(name, node, type);
        }

        boolean requiresCopyFlag = processedQuery.getSelectNodes().stream().anyMatch(node -> !(node instanceof FieldExpressionNode) || processedQuery.hasAlias(node));

        // ^ actually we can check this part first to decide if we do the thing above it or not... heh
        // .. but even if it's passthrough, still need to check all fieldexpnodes. so.
        // if passthrough, dont need to add the nodes to a schema though

        // Check duplicate fields
        List<String> fields = querySchema.getFields().stream().map(Field::getName).collect(Collectors.toList());
        duplicates(fields).ifPresent(duplicates -> {
            errors.add(new BulletError("The following field names are shared: " + duplicates,
                                       "Please specify non-overlapping field names."));
        });

        if (requiresCopyFlag) {
            projection = new Projection(querySchema.getFields(), true);
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
                                                                                          .map(node -> ((FieldExpressionNode) node).getKey().getValue())
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

        /*
        select node

        1) iterate

        if all fields, then can be pass_through.

        otherwise, no_copy, project all.

        */

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
            querySchema.addProjectionField(name, type);
            // ^ goood for potential passthrough. i guess that's the reason

            fields.put(name, aliasOrName);

            // Check primitive type
            if (!Type.isUnknown(type) && !Type.isPrimitive(type)) {
                processedQuery.getErrors().add(new BulletError(node.getLocation() + "The SELECT DISTINCT field " + node + " is non-primitive. Type given: " + type,
                                                               "Please specify primitive fields only for SELECT DISTINCT."));
            }
        }

        boolean requiresNoCopyFlag = processedQuery.getSelectNodes().stream().anyMatch(node -> !(node instanceof FieldExpressionNode));

        if (requiresNoCopyFlag) {
            projection = new Projection(querySchema.getFields(), false);
        } else {
            projection = new Projection();
        }

        querySchema.nextLevel(true);

        /*
        aggregation

        group_by.

        all select nodes -> get alias if there or just name. getaliasorname

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
/*
    private static Map<String, String> toAliasedFields(ProcessedQuery processedQuery, Collection<ExpressionNode> expressions) {
        return expressions.stream().collect(Collectors.toMap(ExpressionNode::getName, processedQuery::getAliasOrName, throwingMerger(), HashMap::new));
    }

    private static <T> BinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        };
    }
*/
    public void doGroup() {


        /*
        select nodes

        1) iterate

        the aggregate / contains aggregate fields need to be ignored... kinda?

        a) projection fields (i.e. preagg)
        b) aggregation field (i.e. agg)
        c) computations (i.e. postagg)

        ^ means we'd have to keep the isAggregateNode / isSuperAggregateNode sets...

        is there any way to get past that and doing something "cleaner" / less janky?

        could be tagged in "select item node"

        could we process it all at once as we go?

        wouldn't that be more confusing.............

        let's think about it.


        //

        if projection field...... should be pretty standard.

        agg field........ gotta read and find group op nodes and find expression inside...

        super agg field...... gotta read and find group op nodes and find expression inside...

         */

        /*
        aggregation / group by nodes

        post agg mapping

        validate on second layer of schema.

        apply alias
         */


        /*
        Aggregation aggregation;
        Set<GroupOperation> operations = processedQuery.getGroupOpNodes().stream().map(node -> {
            String field = node.getOp() != COUNT ? node.getExpression().getName() : null;
            return new GroupOperation(node.getOp(), field, processedQuery.getAliasOrName(node));
        }).collect(Collectors.toSet());
        if (!processedQuery.getGroupByNodes().isEmpty()) {
            aggregation = new GroupBy(processedQuery.getLimit(), toAliasedFields(processedQuery, processedQuery.getGroupByNodes()), operations);
        } else {
            aggregation = new GroupAll(operations);
        }
        addSimplePostAggregationMapping(processedQuery, processedQuery.getGroupByNodes());
        addComplexPostAggregationMapping(processedQuery, processedQuery.getGroupByNodes());
        addPostAggregationMapping(processedQuery, processedQuery.getGroupOpNodes());
        */


        // having

        // computation

        // order by

        // transient

    }

    public void doCountDistinct() {
        /*
        select count(distinct of some expression here, could be multiple, damn) + could be in an expression, and have other random shit where
         */


        /*

        select nodes are totally useless right here

        projection comes from expressions from count distinct..






         */




        /*
        CountDistinctNode countDistinct = processedQuery.getCountDistinct();
        List<String> fields = countDistinct.getExpressions().stream().map(ExpressionNode::getName).collect(Collectors.toCollection(ArrayList::new));
        CountDistinct aggregation = new CountDistinct(fields, processedQuery.getAliasOrName(countDistinct));
        addPostAggregationMapping(processedQuery, countDistinct);
         */

        // no having

        // computation

        // order by

        // possible transient if count(distinct .......) was never selected
    }

    public void doDistribution() {

        /*

        select distribution(of this possible expression), only computations here afterward

         */

        processedQuery.getDistribution().getAggregation(processedQuery.getLimit());

        // no having or transient

        // computation

        // order by

    }

    public void doTopK() {

        /*


         */

        /*
        TopKNode topK = processedQuery.getTopK();
        Map<String, String> fields = toAliasedFields(processedQuery, topK.getExpressions());
        addSimplePostAggregationMapping(processedQuery, topK.getExpressions());
        addComplexPostAggregationMapping(processedQuery, topK.getExpressions());
        return new TopK(fields, topK.getSize(), topK.getThreshold(), processedQuery.getAlias(topK));
         */

    }

    public void doSpecialK() {

        /*


         */

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
    }

    /*
    public Query getQuery() {
        return new Query(projection, filter, aggregation, postAggregations, window, duration);
    }
    */
}
