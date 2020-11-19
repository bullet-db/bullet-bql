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
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.Operation;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
        TOP_K,
        SPECIAL_K,
        INVALID
    }

    // Enum hashset of bullet errors


    @Getter
    @AllArgsConstructor
    public enum QueryError {
        MULTIPLE_QUERY_TYPES(new BulletError("Query does not match exactly one query type.", "Please specify a valid query.")),
        EMPTY_ALIAS(new BulletError("Cannot have an empty string as an field.", "Please specify a non-empty string instead.")),
        MULTIPLE_ALIAS(new BulletError("Cannot have the same alias used for multiple fields.", "Please specify unique aliases.")),
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
        NON_POSITIVE_LIMIT(new BulletError("LIMIT clause must be positive.", "Please specify a positive LIMIT clause."));

        private BulletError error;
    }


    private static final String DELIMITER = ", ";

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

    private Map<ExpressionNode, Expression> preAggregationMapping = new HashMap<>();
    private Map<ExpressionNode, Expression> postAggregationMapping = new HashMap<>();

    private Map<ExpressionNode, String> aliases = new HashMap<>();
    private Set<ExpressionNode> subExpressionNodes = new HashSet<>();

    private Set<ExpressionNode> selectNodes = new LinkedHashSet<>();
    private Set<ExpressionNode> groupByNodes = new LinkedHashSet<>();
    private Set<ExpressionNode> orderByNodes = new LinkedHashSet<>();
    private Set<SortItemNode> sortItemNodes = new LinkedHashSet<>();
    private Set<ExpressionNode> orderByExtraSelectNodes = new LinkedHashSet<>();
    //@Setter
    //private Set<String> selectNames;


    private Set<SelectItemNode> selectItemNodes = new LinkedHashSet<>();


    private Set<ExpressionNode> superAggregateNodes = new HashSet<>();
    private Set<ExpressionNode> aggregateNodes = new HashSet<>();
    private Set<GroupOperationNode> groupOpNodes = new HashSet<>();
    private Set<CountDistinctNode> countDistinctNodes = new HashSet<>();
    private Set<DistributionNode> distributionNodes = new HashSet<>();
    private Set<TopKNode> topKNodes = new HashSet<>();

    private List<BulletError> errors = new ArrayList<>();
    @Setter
    private Collection<ExpressionNode> projection;
    @Setter
    private Collection<ExpressionNode> computation;

    /**
     * Validates the query components.
     *
     * @return This {@link ProcessedQuery} for convenience.
     */
    public ProcessedQuery validate() {
        if (queryTypeSet.size() != 1) {
            errors.add(new BulletError("Query does not match exactly one query type: " + queryTypeSet.stream().sorted().collect(Collectors.toList()),
                                       "Please specify a valid query."));
        }
        if (aliases.values().contains("")) {
            errors.add(new BulletError("Cannot have an empty string as an field.", "Please specify a non-empty string instead."));
        }
        if (aggregateNodes.stream().anyMatch(this::isSuperAggregate)) {
            errors.add(new BulletError("Aggregates cannot be nested.", "Please remove any nested aggregates."));
        }
        if (whereNode != null && isAggregateOrSuperAggregate(whereNode)) {
            errors.add(new BulletError("WHERE clause cannot contain aggregates.", "If you wish to filter on an aggregate, please specify it in the HAVING clause."));
        }
        if (groupByNodes.stream().anyMatch(this::isAggregateOrSuperAggregate)) {
            errors.add(new BulletError("GROUP BY clause cannot contain aggregates.", "Please remove any aggregates from the GROUP BY clause."));
        }
        if (!countDistinctNodes.isEmpty()) {
            if (countDistinctNodes.size() > 1) {
                errors.add(new BulletError("Cannot have multiple COUNT DISTINCT.", "Please specify only one COUNT DISTINCT."));
            }
            if (!orderByNodes.isEmpty()) {
                errors.add(new BulletError("ORDER BY clause is not supported for queries with COUNT DISTINCT.", "Please remove the ORDER BY clause."));
            }
            if (limit != null) {
                errors.add(new BulletError("LIMIT clause is not supported for queries with COUNT DISTINCT.", "Please remove the LIMIT clause."));
            }
        }
        if (!distributionNodes.isEmpty()) {
            if (distributionNodes.size() > 1) {
                errors.add(new BulletError("Cannot have multiple distribution functions.", "Please specify only one distribution function."));
            }
            if (distributionNodes.stream().anyMatch(subExpressionNodes::contains)) {
                errors.add(new BulletError("Distribution functions cannot be treated as values.", Arrays.asList("Please consider using the distribution's output fields instead.",
                                                                                                                "For QUANTILE distributions, the output fields are: [\"Value\", \"Quantile\"].",
                                                                                                                "For FREQ and CUMFREQ distributions, the output fields are: [\"Probability\", \"Count\", \"Quantile\"].")));
            }
        }
        if (!topKNodes.isEmpty()) {
            if (topKNodes.size() > 1) {
                errors.add(new BulletError("Cannot have multiple TOP functions.", "Please specify only one TOP function."));
            }
            if (topKNodes.stream().anyMatch(subExpressionNodes::contains)) {
                errors.add(new BulletError("TOP function cannot be treated as a value.", Arrays.asList("Please consider using the TOP function's output field instead. The default name is \"Count\".",
                                                                                                       "The output field can also be renamed by selecting TOP with an field.")));
            }
            if (!orderByNodes.isEmpty()) {
                errors.add(new BulletError("ORDER BY clause is not supported for queries with a TOP function.", "Please remove the ORDER BY clause."));
            }
            if (limit != null) {
                errors.add(new BulletError("LIMIT clause is not supported for queries with a TOP function.", "Please remove the LIMIT clause."));
            }
        }
        if (havingNode != null && groupByNodes.isEmpty()) {
            errors.add(new BulletError("HAVING clause is only supported with GROUP BY clause.", "Please remove the HAVING clause, and consider using a WHERE clause instead."));
        }
        if (limit != null && limit <= 0) {
            errors.add(new BulletError("LIMIT clause must be positive.", "Please specify a positive LIMIT clause."));
        }
        if (isSpecialK()) {
            queryTypeSet = Collections.singleton(QueryType.SPECIAL_K);
        }
        return this;
    }

    private boolean isSpecialK() {
        if (getQueryType() != QueryType.GROUP || groupByNodes.isEmpty() || groupOpNodes.size() != 1 || sortItemNodes.size() != 1 || limit == null) {
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
        String alias = aliases.get(groupOperationNode);
        SortItemNode sortItemNode = sortItemNodes.iterator().next();
        ExpressionNode orderByNode = sortItemNode.getExpression();
        if (!(orderByNode.equals(groupOperationNode) || isSimpleAliasFieldExpressionMatchingAlias(orderByNode, alias)) || sortItemNode.getOrdering() != SortItemNode.Ordering.DESCENDING) {
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
        return (having.getLeft().equals(groupOperationNode) || isSimpleAliasFieldExpressionMatchingAlias(having.getLeft(), alias)) &&
               having.getOp() == Operation.GREATER_THAN_OR_EQUALS &&
               having.getRight() instanceof LiteralNode &&
               ((LiteralNode) having.getRight()).getValue() instanceof Number;
    }

    void addExpression(ExpressionNode node, ExpressionNode subNode) {
        subExpressionNodes.add(subNode);
        if (isAggregateOrSuperAggregate(subNode)) {
            superAggregateNodes.add(node);
        }
    }

    void addExpression(ExpressionNode node, List<ExpressionNode> subNodes) {
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
     * Returns whether or not the given node is an aggregate.
     *
     * @param node An {@link ExpressionNode}.
     * @return True if the given node is an aggregate and false otherwise.
     */
    public boolean isAggregate(ExpressionNode node) {
        return aggregateNodes.contains(node);
    }

    public boolean isNotAggregate(ExpressionNode node) {
        return !isAggregate(node);
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
        return node instanceof FieldExpressionNode;
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
     * Returns whether or not the given node is a simple field expression that references an field.
     *
     * @param node An {@link ExpressionNode}.
     * @return True if the given node is a simple field expression that references an field and false otherwise.
     */
    public boolean isSimpleAliasFieldExpression(ExpressionNode node) {
        return isSimpleFieldExpression(node) && isAlias(((FieldExpressionNode) node).getField().getValue());
    }

    private boolean isSimpleAliasFieldExpressionMatchingAlias(ExpressionNode expressionNode, String alias) {
        return isSimpleAliasFieldExpression(expressionNode) && ((FieldExpressionNode) expressionNode).getField().getValue().equals(alias);
    }
}
