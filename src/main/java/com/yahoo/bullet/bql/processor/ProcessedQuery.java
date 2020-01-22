package com.yahoo.bullet.bql.processor;

import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.BinaryExpressionNode;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.LiteralNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.parsing.expressions.Operation;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
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
        SPECIAL_K
    }

    private Set<QueryType> queryTypeSet = EnumSet.noneOf(QueryType.class);

    @Setter
    private Long timeDuration;
    @Setter
    private Long recordDuration;
    @Setter
    private Integer limit;

    @Setter
    private boolean windowed;
    @Setter
    private Long emitEvery;
    @Setter
    private Window.Unit emitType;
    @Setter
    private Long first;
    @Setter
    private Window.Unit includeUnit;

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

    public ProcessedQuery validate() {
        if (queryTypeSet.size() != 1) {
            throw new ParsingException("Query matches more than one query type: " + queryTypeSet);
        }
        if (countDistinctNodes.size() > 1) {
            throw new ParsingException("Cannot have multiple count distincts.");
        }
        if (distributionNodes.size() > 1) {
            throw new ParsingException("Cannot have multiple distributions.");
        }
        if (topKNodes.size() > 1) {
            throw new ParsingException("Cannot have multiple top k.");
        }
        if (aggregateNodes.stream().anyMatch(this::isSuperAggregate)) {
            throw new ParsingException("Aggregates cannot be nested.");
        }
        if (distributionNodes.stream().anyMatch(subExpressionNodes::contains)) {
            throw new ParsingException("Distributions cannot be treated as values.");
        }
        if (topKNodes.stream().anyMatch(subExpressionNodes::contains)) {
            throw new ParsingException("Top k cannot be treated as a value.");
        }
        if (whereNode != null && isAggregateOrSuperAggregate(whereNode)) {
            throw new ParsingException("WHERE clause cannot contain aggregates.");
        }
        if (groupByNodes.stream().anyMatch(this::isAggregateOrSuperAggregate)) {
            throw new ParsingException("GROUP BY clause cannot contain aggregates.");
        }
        if (recordDuration != null) {
            throw new ParsingException("STREAM does not currently support record duration.");
        }
        QueryType queryType = getQueryType();
        if (havingNode != null && queryType != QueryType.GROUP) {
            throw new ParsingException("HAVING clause is only supported for queries with group by operations.");
        }
        if (queryType == QueryType.TOP_K || queryType == QueryType.COUNT_DISTINCT) {
            if (!orderByNodes.isEmpty()) {
                throw new ParsingException("ORDER BY clause is not supported for queries with top k or count distinct.");
            }
            if (limit != null) {
                throw new ParsingException("LIMIT clause is not supported for queries with top k or count distinct.");
            }
        }
        setIfSpecialK();
        return this;
    }

    private void setIfSpecialK() {
        if (getQueryType() != QueryType.GROUP || groupByNodes.isEmpty() || groupOpNodes.size() != 1 || orderByNodes.size() != 1 || limit == null) {
            return;
        }
        GroupOperationNode groupOperationNode = groupOpNodes.iterator().next();
        if (groupOperationNode.getOp() != COUNT) {
            return;
        }
        Set<ExpressionNode> selectExpressions = selectNodes.stream().map(SelectItemNode::getExpression).collect(Collectors.toSet());
        if (!selectExpressions.contains(groupOperationNode) || !selectExpressions.containsAll(groupByNodes)) {
            return;
        }
        // Compare by expression since both should point to the same field expression
        Expression groupOperationExpression = getExpression(groupOperationNode);
        SortItemNode sortItemNode = orderByNodes.get(0);
        if (!getExpression(sortItemNode.getExpression()).equals(groupOperationExpression) || sortItemNode.getOrdering() != SortItemNode.Ordering.DESCENDING) {
            return;
        }
        if (havingNode != null) {
            // Check if HAVING has the form: count(*) >= number
            if (!(havingNode instanceof BinaryExpressionNode)) {
                return;
            }
            BinaryExpressionNode having = (BinaryExpressionNode) havingNode;
            if (!getExpression(having.getLeft()).equals(groupOperationExpression) ||
                having.getOp() != Operation.GREATER_THAN_OR_EQUALS ||
                !(having.getRight() instanceof LiteralNode) ||
                !(((LiteralNode) having.getRight()).getValue() instanceof Number)) {
                return;
            }
        }
        queryTypeSet = Collections.singleton(QueryType.SPECIAL_K);
    }

    public QueryType getQueryType() {
        return queryTypeSet.iterator().next();
    }

    public CountDistinctNode getCountDistinct() {
        return countDistinctNodes.iterator().next();
    }

    public DistributionNode getDistribution() {
        return distributionNodes.iterator().next();
    }

    public TopKNode getTopK() {
        return topKNodes.iterator().next();
    }

    public Expression getExpression(ExpressionNode node) {
        return expressionNodes.get(node);
    }

    public String getAlias(ExpressionNode node) {
        return aliases.get(node);
    }

    public String getAliasOrName(ExpressionNode node) {
        String alias = aliases.get(node);
        return alias != null ? alias : node.getName();
    }
/*
    public List<SelectItemNode> getSuperAggregateSelectNodes() {
        return selectNodes.stream().filter(node -> isSuperAggregate(node.getExpression())).collect(Collectors.toList());
    }
*/
    public List<SelectItemNode> getNonAggregateSelectNodes() {
        return selectNodes.stream().filter(node -> !isAggregate(node.getExpression())).collect(Collectors.toList());
    }

    public boolean isAggregateOrSuperAggregate(ExpressionNode node) {
        return aggregateNodes.contains(node) || superAggregateNodes.contains(node);
    }

    public boolean isAggregate(ExpressionNode node) {
        return aggregateNodes.contains(node);
    }

    public boolean isSuperAggregate(ExpressionNode node) {
        return superAggregateNodes.contains(node);
    }

    public boolean isNotGroupByNode(ExpressionNode node) {
        return !groupByNodes.contains(node);
    }

    public boolean isSimpleFieldExpression(ExpressionNode node) {
        Expression expression = expressionNodes.get(node);
        if (!(expression instanceof FieldExpression)) {
            return false;
        }
        FieldExpression fieldExpression = (FieldExpression) expression;
        return fieldExpression.getIndex() == null && fieldExpression.getKey() == null;
    }

    public boolean isNotSimpleFieldExpression(ExpressionNode node) {
        Expression expression = expressionNodes.get(node);
        if (!(expression instanceof FieldExpression)) {
            return true;
        }
        FieldExpression fieldExpression = (FieldExpression) expression;
        return fieldExpression.getIndex() != null || fieldExpression.getKey() != null;
    }

    public boolean isNotFieldExpression(ExpressionNode node) {
        return !(expressionNodes.get(node) instanceof FieldExpression);
    }

    public boolean isNotSimpleAliasFieldExpression(ExpressionNode node) {
        Expression expression = expressionNodes.get(node);
        if (!(expression instanceof FieldExpression)) {
            return true;
        }
        FieldExpression fieldExpression = (FieldExpression) expression;
        return fieldExpression.getIndex() != null ||
               fieldExpression.getKey() != null ||
               !aliases.values().contains(fieldExpression.getField());
    }
}
