/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.BQLConfig;
import com.yahoo.bullet.bql.query.ExpressionProcessor;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Query;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryExtractor {
    private final Long queryMaxDuration;
    private final ExpressionProcessor expressionProcessor;

    /**
     * Constructs a QueryExtractor from a {@link BQLConfig}.
     *
     * @param bqlConfig A {@link BQLConfig}.
     */
    public QueryExtractor(BQLConfig bqlConfig) {
        queryMaxDuration = bqlConfig.getAs(BulletConfig.QUERY_MAX_DURATION, Long.class);
        expressionProcessor = new ExpressionProcessor();
    }

    /**
     * Creates a {@link Query} from the given {@link ProcessedQuery}.
     *
     * @param processedQuery The query components to create a query from.
     * @return A {@link Query} created from the given {@link ProcessedQuery}.
     */
    public Query extractQuery(ProcessedQuery processedQuery) {
        Query query = new Query();

        // extract aggregation

        // validate filter by schema and create filter expression

        // validate projections by schema and create expressions

        // extract aggregation
        // validate aggregation

        // create expression node -> field expression mapping for computations; type carry-over

        //




        extractFilter(processedQuery, query, expressionProcessor);
        extractProjection(processedQuery, query, expressionProcessor);
        extractAggregation(processedQuery, query);
        extractPostAggregations(processedQuery, query, expressionProcessor);
        extractWindow(processedQuery, query);
        extractDuration(processedQuery, query, queryMaxDuration);

        return query;
    }

    private static void extractFilter(ProcessedQuery processedQuery, Query query, ExpressionProcessor expressionProcessor) {
        if (processedQuery.getWhereNode() != null) {
            query.setFilter(expressionProcessor.process(processedQuery.getWhereNode()));
        }
    }

    private static void extractProjection(ProcessedQuery processedQuery, Query query, ExpressionProcessor expressionProcessor) {
        query.setProjection(ProjectionExtractor.extractProjection(processedQuery, expressionProcessor));
    }

    private static void extractAggregation(ProcessedQuery processedQuery, Query query) {
        query.setAggregation(AggregationExtractor.extractAggregation(processedQuery));
    }

    private static void extractPostAggregations(ProcessedQuery processedQuery, Query query, ExpressionProcessor expressionProcessor) {
        query.setPostAggregations(PostAggregationExtractor.extractPostAggregations(processedQuery, expressionProcessor));
    }

    private static void extractWindow(ProcessedQuery processedQuery, Query query) {
        if (processedQuery.getWindow() != null) {
            query.setWindow(WindowExtractor.extractWindow(processedQuery));
        }
    }

    private static void extractDuration(ProcessedQuery processedQuery, Query query, Long queryMaxDuration) {
        if (processedQuery.getTimeDuration() != null) {
            query.setDuration(Math.min(processedQuery.getTimeDuration(), queryMaxDuration));
        }
    }
}
