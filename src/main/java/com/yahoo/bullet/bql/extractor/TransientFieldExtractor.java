package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.processor.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.bql.tree.SelectItemNode;
import com.yahoo.bullet.bql.tree.SortItemNode;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransientFieldExtractor {
    private ProcessedQuery processedQuery;

    public Set<String> extractTransientFields(ProcessedQuery processedQuery) {
        this.processedQuery = processedQuery;
        switch (processedQuery.getQueryType()) {
            case SELECT:
                return extractSelect();
            case SELECT_ALL:
                return extractAll();
            case SELECT_DISTINCT:
                return extractDistinct();
            case GROUP:
            case COUNT_DISTINCT:
                return extractAggregate();
            case DISTRIBUTION:
                return extractDistribution();
            case TOP_K:
                return extractTopK();
            case SPECIAL_K:
                // Doesn't have transient fields since selected fields and order by fields are fixed
                return null;
        }
        throw new ParsingException("Unreachable");
    }

    /**
     * Remove every order by field that's not a select field.
     */
    private Set<String> extractSelect() {
        Set<String> orderByFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                             .map(processedQuery::getAliasOrName)
                                                                             .collect(Collectors.toSet());
        orderByFields.removeAll(getSelectExpressionNames());
        return orderByFields;
    }

    private Set<String> extractAll() {
        return getDisjointedComplexOrderByFields();
    }

    /**
     * Remove every order by field that's not a select field, but don't remove simple field expressions.
     */
    private Set<String> extractDistinct() {
        return getDisjointedComplexOrderByFields();
    }

    /**
     * Remove every field that's not a select field.
     */
    private Set<String> extractAggregate() {
        Set<String> transientFields =
                Stream.concat(Stream.concat(processedQuery.getGroupByNodes().stream(),
                                            processedQuery.getAggregateNodes().stream()),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
                        .map(processedQuery::getAliasOrName)
                        .collect(Collectors.toSet());
        transientFields.removeAll(getSelectExpressionNames());
        return transientFields;
    }

    private Set<String> extractDistribution() {
        return getDisjointedComplexOrderByFields();
    }

    private Set<String> extractTopK() {
        return getDisjointedComplexOrderByFields();
    }

    /**
     * Remove every order by field that's not a select field, but don't remove simple field expressions.
     */
    private Set<String> getDisjointedComplexOrderByFields() {
        Set<String> orderByFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                             .filter(processedQuery::isNotSimpleFieldExpression)
                                                                             .map(processedQuery::getAliasOrName)
                                                                             .collect(Collectors.toSet());
        orderByFields.removeAll(getSelectExpressionNames());
        return orderByFields;
    }

    private Set<String> getSelectExpressionNames() {
        return processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                       .map(processedQuery::getAliasOrName)
                                                       .collect(Collectors.toSet());
    }

    private Set<ExpressionNode> getSelectExpressions() {
        return processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                       .collect(Collectors.toSet());
    }
}
