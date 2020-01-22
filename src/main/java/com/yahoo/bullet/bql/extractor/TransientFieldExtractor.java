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
                return extractAggregateValue();
            case DISTRIBUTION:
            case TOP_K:
                return extractAggregateNonValue();
            case SPECIAL_K:
                return null;
        }
        throw new ParsingException("Unreachable");
    }

    private Set<String> extractSelect() {
        Set<String> orderByFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                             .map(processedQuery::getAliasOrName)
                                                                             .collect(Collectors.toSet());
        orderByFields.removeAll(getAliasSelectFields());
        return orderByFields;
    }

    private Set<String> extractAll() {
        Set<String> orderByFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                             .filter(processedQuery::isNotFieldExpression)
                                                                             .map(processedQuery::getAliasOrName)
                                                                             .collect(Collectors.toSet());
        orderByFields.removeAll(getAliasSelectFields());
        return orderByFields;
    }

    private Set<String> extractDistinct() {
        Set<String> orderByFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                             .filter(processedQuery::isNotFieldExpression)
                                                                             .map(processedQuery::getAliasOrName)
                                                                             .collect(Collectors.toSet());
        orderByFields.removeAll(getAliasSelectFields());
        return orderByFields;
    }

    private Set<String> extractAggregateValue() {
        //Set<String> transientFields =
        //        Stream.concat(Stream.concat(processedQuery.getGroupByNodes().stream(),
        //                                    processedQuery.getAggregateNodes().stream()),
        //                      processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
        //                .map(processedQuery::getAliasOrName)
        //                .collect(Collectors.toSet());
        //transientFields.removeAll(getNonAliasSelectFields());

        Set<ExpressionNode> transientFields =
                Stream.concat(Stream.concat(processedQuery.getGroupByNodes().stream(),
                                            processedQuery.getAggregateNodes().stream()),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
                        .filter(processedQuery::isNotSimpleAliasFieldExpression)
                        .collect(Collectors.toSet());
        transientFields.removeAll(getSelectExpressions());
        return transientFields.stream().map(processedQuery::getAliasOrName).collect(Collectors.toSet());
        //                .map(processedQuery::getAliasOrName)
        //                .collect(Collectors.toSet());
        //transientFields.removeAll(getNonAliasSelectFields());
        //return transientFields;
    }

    private Set<String> extractAggregateNonValue() {
        // TODO same as extractSelectAll()
        Set<String> orderByFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                             .map(processedQuery::getAliasOrName)
                                                                             .collect(Collectors.toSet());
        orderByFields.removeAll(getAliasSelectFields());
        return orderByFields;
    }

    private Set<String> getAliasSelectFields() {
        return processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                       .map(processedQuery::getAliasOrName)
                                                       .collect(Collectors.toSet());
    }

    private Set<ExpressionNode> getSelectExpressions() {
        return processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                       .collect(Collectors.toSet());
    }
}
