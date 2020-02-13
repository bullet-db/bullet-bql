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
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.parsing.expressions.Operation;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.COUNT;

@Getter
public class ProcessedQuery {
    public enum QueryType {
        TOP_K,
        DISTRIBUTION,
        COUNT_DISTINCT,
        GROUP,
        SELECT_DISTINCT,
        SELECT_ALL,
        SELECT,
        SPECIAL_K,
        INVALID
    }

    private Set<QueryType> queryTypeSet = EnumSet.noneOf(QueryType.class);

    @Setter
    private Long timeDuration;
    @Setter
    private Integer limit;
    @Setter
    private WindowNode window;
    @Setter
    private ExpressionNode whereNode;
    @Setter
    private ExpressionNode havingNode;

    private Map<ExpressionNode, Expression> expressionNodes = new HashMap<>();
    private Map<ExpressionNode, String> aliases = new HashMap<>();
    private Set<ExpressionNode> subExpressionNodes = new HashSet<>();

    private List<SelectItemNode> selectNodes = new ArrayList<>();
    private List<ExpressionNode> groupByNodes = new ArrayList<>();
    private List<SortItemNode> orderByNodes = new ArrayList<>();

    private Set<ExpressionNode> superAggregateNodes = new HashSet<>();
    private Set<ExpressionNode> aggregateNodes = new HashSet<>();
    private Set<GroupOperationNode> groupOpNodes = new HashSet<>();
    private Set<CountDistinctNode> countDistinctNodes = new HashSet<>();
    private Set<DistributionNode> distributionNodes = new HashSet<>();
    private Set<TopKNode> topKNodes = new HashSet<>();

    private List<BulletError> errors = new ArrayList<>();
    @Setter
    private Collection<ExpressionNode> projectionNodes;
    @Setter
    private Collection<ExpressionNode> computationNodes;

    /**
     * Validates the query components.
     *
     * @return This {@link ProcessedQuery} for convenience.
     */
    public ProcessedQuery validate() {
        if (queryTypeSet.size() != 1) {
            errors.add(new BulletError("Query does not match exactly one query type: " + queryTypeSet, null));
        }
        if (aliases.values().contains("")) {
            errors.add(new BulletError("Cannot have an empty string as an alias.", null));
        }
        if (countDistinctNodes.size() > 1) {
            errors.add(new BulletError("Cannot have multiple count distincts.", null));
        }
        if (distributionNodes.size() > 1) {
            errors.add(new BulletError("Cannot have multiple distributions.", null));
        }
        if (topKNodes.size() > 1) {
            errors.add(new BulletError("Cannot have multiple top k.", null));
        }
        if (aggregateNodes.stream().anyMatch(this::isSuperAggregate)) {
            errors.add(new BulletError("Aggregates cannot be nested.", null));
        }
        if (distributionNodes.stream().anyMatch(subExpressionNodes::contains)) {
            errors.add(new BulletError("Distributions cannot be treated as values.", null));
        }
        if (topKNodes.stream().anyMatch(subExpressionNodes::contains)) {
            errors.add(new BulletError("Top k cannot be treated as a value.", null));
        }
        if (whereNode != null && isAggregateOrSuperAggregate(whereNode)) {
            errors.add(new BulletError("WHERE clause cannot contain aggregates.", null));
        }
        if (groupByNodes.stream().anyMatch(this::isAggregateOrSuperAggregate)) {
            errors.add(new BulletError("GROUP BY clause cannot contain aggregates.", null));
        }
        if (havingNode != null && groupByNodes.isEmpty()) {
            errors.add(new BulletError("HAVING clause is only supported with GROUP BY clause.", null));
        }
        if (limit != null && limit <= 0) {
            errors.add(new BulletError("LIMIT clause must be positive.", null));
        }
        if (!countDistinctNodes.isEmpty()) {
            if (!orderByNodes.isEmpty()) {
                errors.add(new BulletError("ORDER BY clause is not supported for queries with count distinct.", null));
            }
            if (limit != null) {
                errors.add(new BulletError("LIMIT clause is not supported for queries with count distinct.", null));
            }
        }
        if (!topKNodes.isEmpty()) {
            if (!orderByNodes.isEmpty()) {
                errors.add(new BulletError("ORDER BY clause is not supported for queries with top k.", null));
            }
            if (limit != null) {
                errors.add(new BulletError("LIMIT clause is not supported for queries with top k.", null));
            }
        }
        if (isSpecialK()) {
            queryTypeSet = Collections.singleton(QueryType.SPECIAL_K);
        }
        return this;
    }

    private boolean isSpecialK() {
        if (getQueryType() != QueryType.GROUP || groupByNodes.isEmpty() || groupOpNodes.size() != 1 || orderByNodes.size() != 1 || limit == null) {
            return false;
        }
        GroupOperationNode groupOperationNode = groupOpNodes.iterator().next();
        if (groupOperationNode.getOp() != COUNT) {
            return false;
        }
        Set<ExpressionNode> selectExpressions = selectNodes.stream().map(SelectItemNode::getExpression).collect(Collectors.toSet());
        if (!selectExpressions.contains(groupOperationNode) || !selectExpressions.containsAll(groupByNodes)) {
            return false;
        }
        // Compare by expression since both should point to the same field expression
        Expression groupOperationExpression = getExpression(groupOperationNode);
        SortItemNode sortItemNode = orderByNodes.get(0);
        if (!getExpression(sortItemNode.getExpression()).equals(groupOperationExpression) || sortItemNode.getOrdering() != SortItemNode.Ordering.DESCENDING) {
            return false;
        }
        // Optional HAVING
        if (havingNode == null) {
            return true;
        }
        // Check if HAVING has the form: COUNT(*) >= number
        if (!(havingNode instanceof BinaryExpressionNode)) {
            return false;
        }
        BinaryExpressionNode having = (BinaryExpressionNode) havingNode;
        return getExpression(having.getLeft()).equals(groupOperationExpression) &&
               having.getOp() == Operation.GREATER_THAN_OR_EQUALS &&
               having.getRight() instanceof LiteralNode &&
               ((LiteralNode) having.getRight()).getValue() instanceof Number;
    }

    void addExpression(ExpressionNode node, Expression expression) {
        expressionNodes.put(node, expression);
    }

    void addExpression(ExpressionNode node, Expression expression, ExpressionNode subNode) {
        addExpression(node, expression);
        subExpressionNodes.add(subNode);
        if (isAggregateOrSuperAggregate(subNode)) {
            superAggregateNodes.add(node);
        }
    }

    void addExpression(ExpressionNode node, Expression expression, List<ExpressionNode> subNodes) {
        expressionNodes.put(node, expression);
        subExpressionNodes.addAll(subNodes);
        if (subNodes.stream().anyMatch(this::isAggregateOrSuperAggregate)) {
            superAggregateNodes.add(node);
        }
    }

    /**
     * Returns the query type.
     *
     * @return A {@link QueryType}.
     */
    public QueryType getQueryType() {
        return errors.isEmpty() ? queryTypeSet.iterator().next() : QueryType.INVALID;
    }

    /**
     * Returns the count distinct node.
     *
     * @return A {@link CountDistinctNode}.
     */
    public CountDistinctNode getCountDistinct() {
        return countDistinctNodes.iterator().next();
    }

    /**
     * Returns the distribution node.
     *
     * @return A {@link DistributionNode}.
     */
    public DistributionNode getDistribution() {
        return distributionNodes.iterator().next();
    }

    /**
     * Returns the top k node.
     *
     * @return A {@link TopKNode}.
     */
    public TopKNode getTopK() {
        return topKNodes.iterator().next();
    }

    /**
     * Returns the expression mapped to the given {@link ExpressionNode}.
     *
     * @param node An {@link ExpressionNode}.
     * @return An {@link Expression}.
     */
    public Expression getExpression(ExpressionNode node) {
        return expressionNodes.get(node);
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
     * Returns the list of select items that are not aggregates.
     *
     * @return The list of select items that are not aggregates.
     */
    public List<SelectItemNode> getNonAggregateSelectNodes() {
        return selectNodes.stream().filter(node -> !isAggregate(node.getExpression())).collect(Collectors.toList());
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
     * Returns whether or not the given node is an aggregate.
     *
     * @param node An {@link ExpressionNode}.
     * @return True if the given node is an aggregate and false otherwise.
     */
    public boolean isAggregate(ExpressionNode node) {
        return aggregateNodes.contains(node);
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

    /**
     * Returns whether or not the given node is not a group by node.
     *
     * @param node An {@link ExpressionNode}.
     * @return True if the given node is not a group by node and false otherwise.
     */
    public boolean isNotGroupByNode(ExpressionNode node) {
        return !groupByNodes.contains(node);
    }

    /**
     * Returns whether or not the given node is a simple field expression.
     *
     * @param node An {@link ExpressionNode}.
     * @return True if the given node is a simple field expression and false otherwise.
     */
    public boolean isSimpleFieldExpression(ExpressionNode node) {
        Expression expression = expressionNodes.get(node);
        if (!(expression instanceof FieldExpression)) {
            return false;
        }
        FieldExpression fieldExpression = (FieldExpression) expression;
        return fieldExpression.getIndex() == null && fieldExpression.getKey() == null;
    }

    /**
     * Returns whether or not the given node is not a simple field expression.
     *
     * @param node An {@link ExpressionNode}.
     * @return True if the given node is not a simple field expression and false otherwise.
     */
    public boolean isNotSimpleFieldExpression(ExpressionNode node) {
        return !isSimpleFieldExpression(node);
    }

    /**
     * Returns whether or not the given node is a simple field expression that references an alias.
     *
     * @param node An {@link ExpressionNode}.
     * @return True if the given node is a simple field expression that references an alias and false otherwise.
     */
    public boolean isSimpleAliasFieldExpression(ExpressionNode node) {
        Expression expression = expressionNodes.get(node);
        if (!(expression instanceof FieldExpression)) {
            return false;
        }
        FieldExpression fieldExpression = (FieldExpression) expression;
        return fieldExpression.getIndex() == null &&
               fieldExpression.getKey() == null &&
               aliases.values().contains(fieldExpression.getField());
    }

    /**
     * Returns whether or not the given node is not a simple field expression that references an alias.
     *
     * @param node An {@link ExpressionNode}.
     * @return True if the given node is not a simple field expression that references an alias and false otherwise.
     */
    public boolean isNotSimpleAliasFieldExpression(ExpressionNode node) {
        return !isSimpleAliasFieldExpression(node);
    }
}
