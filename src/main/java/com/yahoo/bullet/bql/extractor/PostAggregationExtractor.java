/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ExpressionProcessor;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.tree.ExpressionNode;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Culling;
import com.yahoo.bullet.parsing.Having;
import com.yahoo.bullet.parsing.OrderBy;
import com.yahoo.bullet.parsing.PostAggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PostAggregationExtractor {
    static List<PostAggregation> extractPostAggregations(ProcessedQuery processedQuery, ExpressionProcessor expressionProcessor) {
        List<PostAggregation> postAggregations = new ArrayList<>();

        extractHaving(processedQuery, postAggregations, expressionProcessor);
        extractComputations(processedQuery, postAggregations, expressionProcessor);
        extractOrderBy(processedQuery, postAggregations);
        extractTransientFields(processedQuery, postAggregations);

        return !postAggregations.isEmpty() ? postAggregations : null;
    }

    private static void extractHaving(ProcessedQuery processedQuery, List<PostAggregation> postAggregations, ExpressionProcessor expressionProcessor) {
        // Special K has a HAVING clause, but it's subsumed by Top K
        if (processedQuery.getHavingNode() != null && processedQuery.getQueryType() != ProcessedQuery.QueryType.SPECIAL_K) {
            //postAggregations.add(new Having(processedQuery.getExpression(processedQuery.getHavingNode())));
            postAggregations.add(new Having(expressionProcessor.process(processedQuery.getHavingNode(), processedQuery.getAggregateMapping())));
        }
    }

    private static void extractComputations(ProcessedQuery processedQuery, List<PostAggregation> postAggregations, ExpressionProcessor expressionProcessor) {
        Computation computation = ComputationExtractor.extractComputation(processedQuery, expressionProcessor);
        if (computation != null && !computation.getFields().isEmpty()) {
            postAggregations.add(computation);
        }
    }

    /*
    If an order by field references an alias (by itself), then we won't replace it with an alias that might exist, e.g.
    SELECT abc AS def, def AS abc FROM STREAM() ORDER BY abc; will order by "abc" and won't be replaced by "def" .
    */
    private static void extractOrderBy(ProcessedQuery processedQuery, List<PostAggregation> postAggregations) {
        // Special K has an ORDER BY clause, but it's subsumed by Top K
        if (processedQuery.getOrderByNodes().isEmpty() || processedQuery.getQueryType() == ProcessedQuery.QueryType.SPECIAL_K) {
            return;
        }

        List<OrderBy.SortItem> fields = processedQuery.getOrderByNodes().stream().map(node -> {
                OrderBy.SortItem sortItem = new OrderBy.SortItem();
                ExpressionNode expression = node.getExpression();
                if (processedQuery.isSimpleAliasFieldExpression(expression)) {
                    sortItem.setField(expression.getName());
                } else {
                    sortItem.setField(processedQuery.getAliasOrName(expression));
                }
                sortItem.setDirection(node.getOrdering().getDirection());
                return sortItem;
            }).collect(Collectors.toList());

        postAggregations.add(new OrderBy(fields));
    }

    private static void extractTransientFields(ProcessedQuery processedQuery, List<PostAggregation> postAggregations) {
        Set<String> transientFields = TransientFieldExtractor.extractTransientFields(processedQuery);
        if (transientFields != null && !transientFields.isEmpty()) {
            postAggregations.add(new Culling(transientFields));
        }
    }
}
