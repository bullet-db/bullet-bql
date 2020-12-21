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
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.expressions.Operation;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
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
        SELECT_DISTINCT,
        GROUP,
        COUNT_DISTINCT,
        DISTRIBUTION,
        TOP_K
    }

    @Getter
    @AllArgsConstructor
    public enum QueryError {
        MULTIPLE_QUERY_TYPES(new BulletError("Query consists of multiple aggregation types.", "Please specify a valid query with only one aggregation type.")),
        EMPTY_ALIAS(new BulletError("Cannot have an empty string as an alias.", "Please specify a non-empty string instead.")),
        NESTED_AGGREGATE(new BulletError("Aggregates cannot be nested.", "Please remove any nested aggregates.")),
        WHERE_WITH_AGGREGATE(new BulletError("WHERE clause cannot contain aggregates.", "If you wish to filter on an aggregate, please specify it in the HAVING clause.")),
        GROUP_BY_WITH_AGGREGATE(new BulletError("GROUP BY clause cannot contain aggregates.", "Please remove any aggregates from the GROUP BY clause.")),
        MULTIPLE_COUNT_DISTINCT(new BulletError("Cannot have multiple COUNT DISTINCT.", "Please specify only one COUNT DISTINCT.")),
        COUNT_DISTINCT_WITH_ORDER_BY(new BulletError("ORDER BY clause is not supported for queries with COUNT DISTINCT.", "Please remove the ORDER BY clause.")),
        COUNT_DISTINCT_WITH_LIMIT(new BulletError("LIMIT clause is not supported for queries with COUNT DISTINCT.", "Please remove the LIMIT clause.")),
        MULTIPLE_DISTRIBUTION(new BulletError("Cannot have multiple distribution functions.", "Please specify only one distribution function.")),
        DISTRIBUTION_AS_VALUE(new BulletError("Distribution functions cannot be treated as values.", Arrays.asList("Please consider using the distribution's output fields instead.",
                                                                                                                   "For QUANTILE distributions, the output fields are: [\"Value\", \"Quantile\"].",
                                                                                                                   "For FREQ and CUMFREQ distributions, the output fields are: [\"Probability\", \"Count\", \"Quantile\"]."))),
        MULTIPLE_TOP_K(new BulletError("Cannot have multiple TOP functions.", "Please specify only one TOP function.")),
        TOP_K_AS_VALUE(new BulletError("TOP function cannot be treated as a value.", Arrays.asList("Please consider using the TOP function's output field instead. The default name is \"Count\".",
                                                                                                   "The output field can also be renamed by selecting TOP with an field."))),
        TOP_K_WITH_ORDER_BY(new BulletError("ORDER BY clause is not supported for queries with a TOP function.", "Please remove the ORDER BY clause.")),
        TOP_K_WITH_LIMIT(new BulletError("LIMIT clause is not supported for queries with a TOP function.", "Please remove the LIMIT clause.")),
        HAVING_WITHOUT_GROUP_BY(new BulletError("HAVING clause is only supported with GROUP BY clause.", "Please remove the HAVING clause, and consider using a WHERE clause instead.")),
        NON_POSITIVE_DURATION(new BulletError("Query duration must be positive.", "Please specify a positive duration.")),
        NON_POSITIVE_LIMIT(new BulletError("LIMIT clause must be positive.", "Please specify a positive LIMIT clause."));

        private BulletError error;
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
        return queryErrors.isEmpty();
    }

    public List<BulletError> getErrors() {
        return queryErrors.stream().map(QueryError::getError).collect(Collectors.toList());
    }

    public void addQueryType(QueryType queryType) {
        queryTypes.add(queryType);
    }

    public void addSelectNode(ExpressionNode node) {
        if (node instanceof DistributionNode || node instanceof TopKNode) {
            return;
        }
        selectNodes.add(node);
    }

    public void addAlias(ExpressionNode node, String alias) {
        if (alias.isEmpty()) {
            queryErrors.add(QueryError.EMPTY_ALIAS);
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
        return queryTypes.isEmpty() ? QueryType.SELECT : queryTypes.iterator().next();
    }

    /**
     * Returns whether or not the given {@link ExpressionNode} has an field.
     *
     * @param node An {@link ExpressionNode}.
     * @return True if the node has an field and false otherwise.
     */
    public boolean hasAlias(ExpressionNode node) {
        return aliases.containsKey(node);
    }

    /**
     * Returns whether or not the given string is an field.
     *
     * @param name A string.
     * @return True if the given string is an field and false otherwise.
     */
    public boolean isAlias(String name) {
        return aliases.values().contains(name);
    }

    /**
     * Returns the field of the given {@link ExpressionNode}.
     *
     * @param node An {@link ExpressionNode}.
     * @return The field of the given node if it exists and null otherwise.
     */
    public String getAlias(ExpressionNode node) {
        return aliases.get(node);
    }

    /**
     * Returns the field or name of the given {@link ExpressionNode}.
     *
     * @param node An {@link ExpressionNode}.
     * @return The field of the given node if it exists and its name otherwise.
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
