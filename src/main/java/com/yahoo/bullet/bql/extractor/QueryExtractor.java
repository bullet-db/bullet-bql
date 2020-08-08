/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ExpressionProcessor;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.postaggregations.PostAggregation;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class QueryExtractor {
    /**
     * Creates a {@link Query} from the given {@link ProcessedQuery}.
     *
     * @param processedQuery The query components to create a query from.
     * @return A {@link Query} created from the given {@link ProcessedQuery}.
     */
    public static Query extractQuery(ProcessedQuery processedQuery) {
        return new Query(extractProjection(processedQuery),
                         extractFilter(processedQuery),
                         extractAggregation(processedQuery),
                         extractPostAggregations(processedQuery),
                         extractWindow(processedQuery),
                         extractDuration(processedQuery));
    }

    private static Projection extractProjection(ProcessedQuery processedQuery) {
        return ProjectionExtractor.extractProjection(processedQuery);
    }

    private static Expression extractFilter(ProcessedQuery processedQuery) {
        return ExpressionProcessor.visit(processedQuery.getWhereNode(), processedQuery.getPreAggregationMapping());
    }

    private static Aggregation extractAggregation(ProcessedQuery processedQuery) {
        return AggregationExtractor.extractAggregation(processedQuery);
    }

    private static List<PostAggregation> extractPostAggregations(ProcessedQuery processedQuery) {
        return PostAggregationExtractor.extractPostAggregations(processedQuery);
    }

    private static Window extractWindow(ProcessedQuery processedQuery) {
        return WindowExtractor.extractWindow(processedQuery);
    }

    private static Long extractDuration(ProcessedQuery processedQuery) {
        return processedQuery.getTimeDuration();
    }
}
