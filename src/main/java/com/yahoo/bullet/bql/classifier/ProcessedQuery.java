package com.yahoo.bullet.bql.classifier;

import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.CountDistinctNode;
import com.yahoo.bullet.bql.tree.DistributionNode;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.GroupOperationNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.bql.tree.TopKNode;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.parsing.expressions.Expression;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Getter @Setter
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

    private Long timeDuration;
    private Long recordDuration;
    private Integer limit;

    private boolean windowed;
    private Long emitEvery;
    private Window.Unit emitType;
    private Long first;
    private Window.Unit includeUnit;

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

    private ExpressionNode whereNode;
    private ExpressionNode havingNode;

    public ProcessedQuery validate() {
        if (queryTypeSet.size() != 1) {
            throw new ParsingException("Query must match exactly one query type.");
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
        if (distributionNodes.stream().anyMatch(subExpressionNodes::contains) || topKNodes.stream().anyMatch(subExpressionNodes::contains)) {
            throw new ParsingException("Distributions and top k cannot be treated as values.");
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
        // TODO not sure about restriction on HAVING clause
        QueryType queryType = getQueryType();
        if (havingNode != null && queryType != QueryType.GROUP) {
            throw new ParsingException("HAVING clause only supported for queries with group by operations.");
        }
        return this;
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
        return alias != null ? alias : node.toFormatlessString();
    }

    public List<SelectItemNode> getSuperAggregateSelectNodes() {
        return selectNodes.stream().filter(node -> isSuperAggregate(node.getExpression())).collect(Collectors.toList());
    }

    public boolean isAggregateOrSuperAggregate(ExpressionNode node) {
        return aggregateNodes.contains(node) || superAggregateNodes.contains(node);
    }

    public boolean isSuperAggregate(ExpressionNode node) {
        return superAggregateNodes.contains(node);
    }

    public boolean isGroupOrCountDistinct(ExpressionNode node) {
        return (node instanceof GroupOperationNode && groupOpNodes.contains(node)) ||
               (node instanceof CountDistinctNode && countDistinctNodes.contains(node));
    }
}
