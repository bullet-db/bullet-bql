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
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.LateralViewNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.TableFunctionNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.expressions.Operation;
import lombok.Getter;
import lombok.Setter;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.yahoo.bullet.querying.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;

@Getter
public class ProcessedQuery {
    public enum QueryType {
        SELECT,
        SELECT_ALL,
        SELECT_TABLE_FUNCTION,
        SELECT_DISTINCT,
        GROUP,
        COUNT_DISTINCT,
        DISTRIBUTION,
        TOP_K
    }

    private Set<QueryType> queryTypes = EnumSet.noneOf(QueryType.class);
    private Set<QueryError> queryErrors = EnumSet.noneOf(QueryError.class);

    @Setter
    private WindowNode window;
    @Setter
    private ExpressionNode where;
    @Setter
    private ExpressionNode having;
    @Setter
    private Long duration;
    @Setter
    private Integer limit;
    @Setter
    private LateralViewNode lateralView;
    @Setter
    private ProcessedQuery postQuery;

    private Map<ExpressionNode, String> aliases = new HashMap<>();

    // These use LinkedHashSet because order either matters or is nice to have
    private Set<ExpressionNode> selectNodes = new LinkedHashSet<>();
    private Set<ExpressionNode> groupByNodes = new LinkedHashSet<>();
    private Set<ExpressionNode> orderByNodes = new LinkedHashSet<>();
    private Set<SortItemNode> sortItems = new LinkedHashSet<>();

    // Aggregate nodes
    private Set<GroupOperationNode> groupOpNodes = new HashSet<>();
    private CountDistinctNode countDistinct;
    private DistributionNode distribution;
    private TopKNode topK;

    // SELECT'd table function node - currently assumed to be EXPLODE
    private TableFunctionNode selectTableFunction;

    // Record-keeping
    private Set<ExpressionNode> aggregateNodes = new HashSet<>();
    private Set<ExpressionNode> superAggregateNodes = new HashSet<>();
    private Set<ExpressionNode> subExpressionNodes = new HashSet<>();

    /**
     * Validates the query components.
     *
     * @return True if no errors and false otherwise.
     */
    public boolean validate() {
        if (queryTypes.size() > 1) {
            queryErrors.add(QueryError.MULTIPLE_QUERY_TYPES);
        }
        if (aggregateNodes.stream().anyMatch(this::isSuperAggregate)) {
            queryErrors.add(QueryError.NESTED_AGGREGATE);
        }
        if (countDistinct != null) {
            if (!sortItems.isEmpty()) {
                queryErrors.add(QueryError.COUNT_DISTINCT_WITH_ORDER_BY);
            }
            if (limit != null) {
                queryErrors.add(QueryError.COUNT_DISTINCT_WITH_LIMIT);
            }
        }
        if (distribution != null) {
            if (subExpressionNodes.contains(distribution)) {
                queryErrors.add(QueryError.DISTRIBUTION_AS_VALUE);
            }
        }
        if (topK != null) {
            if (subExpressionNodes.contains(topK)) {
                queryErrors.add(QueryError.TOP_K_AS_VALUE);
            }
            if (!sortItems.isEmpty()) {
                queryErrors.add(QueryError.TOP_K_WITH_ORDER_BY);
            }
            if (limit != null) {
                queryErrors.add(QueryError.TOP_K_WITH_LIMIT);
            }
        }
        if (lateralView != null) {
            if (isSuperAggregate(lateralView.getTableFunction())) {
                queryErrors.add(QueryError.TABLE_FUNCTION_WITH_AGGREGATE);
            }
        }
        if (selectTableFunction != null) {
            if (lateralView != null) {
                queryErrors.add(QueryError.SELECT_TABLE_FUNCTION_WITH_LATERAL_VIEW);
            }
            if (!queryTypes.isEmpty()) {
                queryErrors.add(QueryError.SELECT_TABLE_FUNCTION_WITH_AGGREGATION);
            }
            if (isSuperAggregate(selectTableFunction)) {
                queryErrors.add(QueryError.TABLE_FUNCTION_WITH_AGGREGATE);
            }
        }
        if (duration != null && duration <= 0) {
            queryErrors.add(QueryError.NON_POSITIVE_DURATION);
        }
        if (limit != null && limit <= 0) {
            queryErrors.add(QueryError.NON_POSITIVE_LIMIT);
        }
        if (where != null && isAggregateOrSuperAggregate(where)) {
            queryErrors.add(QueryError.WHERE_WITH_AGGREGATE);
        }
        if (having != null && groupByNodes.isEmpty()) {
            queryErrors.add(QueryError.HAVING_WITHOUT_GROUP_BY);
        }
        if (postQuery != null && !postQuery.validate()) {
            queryErrors.addAll(postQuery.getQueryErrors());
        }
        return queryErrors.isEmpty();
    }

    public List<BulletError> getErrors() {
        return queryErrors.stream().map(QueryError::format).collect(Collectors.toList());
    }

    public void addQueryType(QueryType queryType) {
        queryTypes.add(queryType);
    }

    public void addSelectNode(ExpressionNode node) {
        if (node instanceof DistributionNode || node instanceof TopKNode) {
            return;
        }
        if (node instanceof TableFunctionNode) {
            if (selectTableFunction != null && !selectTableFunction.equals(node)) {
                queryErrors.add(QueryError.MULTIPLE_SELECT_TABLE_FUNCTIONS);
                return;
            }
            selectTableFunction = (TableFunctionNode) node;
            return;
        }
        selectNodes.add(node);
    }

    public void addAlias(ExpressionNode node, String alias) {
        if (!(node instanceof TopKNode) && node.getName().equals(alias)) {
            return;
        }
        String existingAlias = aliases.get(node);
        if (existingAlias != null && !existingAlias.equals(alias)) {
            queryErrors.add(QueryError.MULTIPLE_ALIASES);
            return;
        }
        aliases.put(node, alias);
    }

    public void addGroupByNodes(List<ExpressionNode> nodes) {
        if (nodes.stream().anyMatch(this::isAggregateOrSuperAggregate)) {
            queryErrors.add(QueryError.GROUP_BY_WITH_AGGREGATE);
            return;
        }
        groupByNodes.addAll(nodes);
        queryTypes.add(QueryType.GROUP);
    }

    public void addSortItemNode(SortItemNode node) {
        sortItems.add(node);
        orderByNodes.add(node.getExpression());
    }

    void addExpression(ExpressionNode node) {
        List<ExpressionNode> children = node.getChildren();
        subExpressionNodes.addAll(children);
        if (children.stream().anyMatch(this::isAggregateOrSuperAggregate)) {
            superAggregateNodes.add(node);
        }
    }

    void addAggregate(ExpressionNode node) {
        aggregateNodes.add(node);
    }

    public void addGroupOpNode(GroupOperationNode node) {
        groupOpNodes.add(node);
        queryTypes.add(QueryType.GROUP);
    }

    public void setCountDistinct(CountDistinctNode countDistinctNode) {
        if (countDistinct != null && !countDistinct.equals(countDistinctNode)) {
            queryErrors.add(QueryError.MULTIPLE_COUNT_DISTINCT);
            return;
        }
        countDistinct = countDistinctNode;
        queryTypes.add(QueryType.COUNT_DISTINCT);
    }

    public void setDistribution(DistributionNode distributionNode) {
        if (distribution != null && !distribution.equals(distributionNode)) {
            queryErrors.add(QueryError.MULTIPLE_DISTRIBUTION);
            return;
        }
        distribution = distributionNode;
        queryTypes.add(QueryType.DISTRIBUTION);
    }

    public void setTopK(TopKNode topKNode) {
        if (topK != null && !topK.equals(topKNode)) {
            queryErrors.add(QueryError.MULTIPLE_TOP_K);
            return;
        }
        topK = topKNode;
        queryTypes.add(QueryType.TOP_K);
    }

    /**
     * Returns the query type.
     *
     * @return A {@link QueryType}.
     */
    public QueryType getQueryType() {
        if (queryTypes.isEmpty()) {
            return selectTableFunction != null ? QueryType.SELECT_TABLE_FUNCTION : QueryType.SELECT;
        }
        return queryTypes.iterator().next();
    }

    /**
     * Returns whether or not the given {@link ExpressionNode} has an alias.
     *
     * @param node An {@link ExpressionNode}.
     * @return True if the node has an alias and false otherwise.
     */
    public boolean hasAlias(ExpressionNode node) {
        return aliases.containsKey(node);
    }

    /**
     * Returns the alias of the given {@link ExpressionNode}.
     *
     * @param node An {@link ExpressionNode}.
     * @return The alias of the given node if it exists and null otherwise.
     */
    public String getAlias(ExpressionNode node) {
        return aliases.get(node);
    }

    /**
     * Returns the alias or name of the given {@link ExpressionNode}.
     *
     * @param node An {@link ExpressionNode}.
     * @return The alias of the given node if it exists and its name otherwise.
     */
    public String getAliasOrName(ExpressionNode node) {
        String alias = aliases.get(node);
        return alias != null ? alias : node.getName();
    }

    /**
     * Returns whether or not the given node is an aggregate or contains aggregates.
     *
     * @param node An {@link ExpressionNode}.
     * @return True if the given node is an aggregate or contains aggregates and false otherwise.
     */
    public boolean isAggregateOrSuperAggregate(ExpressionNode node) {
        return aggregateNodes.contains(node) || superAggregateNodes.contains(node);
    }

    /**
     * Returns whether or not the given node contains aggregates.
     *
     * @param node An {@link ExpressionNode}.
     * @return True if the given node contains aggregates and false otherwise.
     */
    public boolean isSuperAggregate(ExpressionNode node) {
        return superAggregateNodes.contains(node);
    }

    public boolean isSpecialK() {
        if (getQueryType() != QueryType.GROUP || groupByNodes.isEmpty() || groupOpNodes.size() != 1 || sortItems.size() != 1 || limit == null) {
            return false;
        }
        GroupOperationNode groupOperationNode = groupOpNodes.iterator().next();
        if (groupOperationNode.getOp() != COUNT) {
            return false;
        }
        if (!selectNodes.contains(groupOperationNode) || !selectNodes.containsAll(groupByNodes)) {
            return false;
        }
        // Compare by expression node or field name
        String name = getAliasOrName(groupOperationNode);
        SortItemNode sortItem = sortItems.iterator().next();
        ExpressionNode orderByNode = sortItem.getExpression();
        if (!(orderByNode.equals(groupOperationNode) || orderByNode.getName().equals(name)) || sortItem.getOrdering() != SortItemNode.Ordering.DESCENDING) {
            return false;
        }
        // Optional HAVING
        if (having == null) {
            return true;
        }
        // Check if HAVING has the form: COUNT(*) >= number
        if (!(having instanceof BinaryExpressionNode)) {
            return false;
        }
        BinaryExpressionNode having = (BinaryExpressionNode) this.having;
        return (having.getLeft().equals(groupOperationNode) || having.getLeft().getName().equals(name)) &&
               having.getOp() == Operation.GREATER_THAN_OR_EQUALS &&
               having.getRight() instanceof LiteralNode &&
               ((LiteralNode) having.getRight()).getValue() instanceof Number;
    }
}
