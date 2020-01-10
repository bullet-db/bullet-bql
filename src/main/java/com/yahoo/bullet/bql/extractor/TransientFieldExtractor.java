package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.classifier.ProcessedQuery;
import com.yahoo.bullet.bql.parser.ParsingException;
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
                return extractSelectAll();
            case SELECT_DISTINCT:
                return null;
            case GROUP:
            case COUNT_DISTINCT:
                return extractAggregateValue();
            case DISTRIBUTION:
            case TOP_K:
                return extractAggregateNonValue();
            case SPECIAL_K:
                return null;
        }
        throw new ParsingException("Unsupported");
    }

    private Set<String> extractSelect() {
        Set<String> orderByFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                             .map(processedQuery::getAliasOrName)
                                                                             .collect(Collectors.toSet());
        orderByFields.removeAll(getSelectFields());
        return orderByFields;
    }

    private Set<String> extractSelectAll() {
        Set<String> orderByFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                             .filter(processedQuery::isNotFieldExpression)
                                                                             .map(processedQuery::getAliasOrName)
                                                                             .collect(Collectors.toSet());
        orderByFields.removeAll(getSelectFields());
        return orderByFields;
    }

    private Set<String> extractAggregateValue() {
        Set<String> transientFields =
                Stream.concat(Stream.concat(processedQuery.getGroupByNodes().stream(),
                                            processedQuery.getAggregateNodes().stream()),
                              processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression))
                        .map(processedQuery::getAliasOrName)
                        .collect(Collectors.toSet());
        transientFields.removeAll(getSelectFields());
        return transientFields;
    }

    private Set<String> extractAggregateNonValue() {
        // TODO same as extractSelectAll()
        Set<String> orderByFields = processedQuery.getOrderByNodes().stream().map(SortItemNode::getExpression)
                                                                             .map(processedQuery::getAliasOrName)
                                                                             .collect(Collectors.toSet());
        orderByFields.removeAll(getSelectFields());
        return orderByFields;
    }

    private Set<String> getSelectFields() {
        return processedQuery.getSelectNodes().stream().map(SelectItemNode::getExpression)
                                                       .map(processedQuery::getAliasOrName)
                                                       .collect(Collectors.toSet());
    }
}
