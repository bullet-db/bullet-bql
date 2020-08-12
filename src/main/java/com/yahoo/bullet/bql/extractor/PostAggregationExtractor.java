/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ExpressionProcessor;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.query.postaggregations.Having;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.query.postaggregations.PostAggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PostAggregationExtractor {
    static List<PostAggregation> extractPostAggregations(ProcessedQuery processedQuery) {
        List<PostAggregation> postAggregations = new ArrayList<>();
        extractHaving(processedQuery, postAggregations);
        extractComputations(processedQuery, postAggregations);
        extractOrderBy(processedQuery, postAggregations);
        extractTransientFields(processedQuery, postAggregations);
        return !postAggregations.isEmpty() ? postAggregations : null;
    }

    private static void extractHaving(ProcessedQuery processedQuery, List<PostAggregation> postAggregations) {
        // Special K has a HAVING clause, but it's subsumed by Top K
        if (processedQuery.getHavingNode() != null && processedQuery.getQueryType() != ProcessedQuery.QueryType.SPECIAL_K) {
            postAggregations.add(new Having(ExpressionProcessor.visit(processedQuery.getHavingNode(), processedQuery.getPostAggregationMapping())));
        }
    }

    private static void extractComputations(ProcessedQuery processedQuery, List<PostAggregation> postAggregations) {
        Computation computation = ComputationExtractor.extractComputation(processedQuery);
        if (computation != null) {
            postAggregations.add(computation);
        }
    }

    private static void extractOrderBy(ProcessedQuery processedQuery, List<PostAggregation> postAggregations) {
        // Special K has an ORDER BY clause, but it's subsumed by Top K
        if (processedQuery.getOrderByNodes().isEmpty() || processedQuery.getQueryType() == ProcessedQuery.QueryType.SPECIAL_K) {
            return;
        }
        List<OrderBy.SortItem> fields = processedQuery.getSortItemNodes().stream().map(
                node -> new OrderBy.SortItem(ExpressionProcessor.visit(node.getExpression(), processedQuery.getPostAggregationMapping()),
                                             node.getOrdering().getDirection())
        ).collect(Collectors.toCollection(ArrayList::new));
        postAggregations.add(new OrderBy(fields));
    }

    private static void extractTransientFields(ProcessedQuery processedQuery, List<PostAggregation> postAggregations) {
        Set<String> transientFields = TransientFieldExtractor.extractTransientFields(processedQuery);
        if (transientFields != null && !transientFields.isEmpty()) {
            postAggregations.add(new Culling(transientFields));
        }
    }
}
