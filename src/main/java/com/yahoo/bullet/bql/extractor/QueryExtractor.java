/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.BQLConfig;
import com.yahoo.bullet.bql.processor.ProcessedQuery;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Query;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryExtractor {
    private final Long queryMaxDuration;

    /**
     * Constructs a QueryExtractor from a {@link BQLConfig}.
     *
     * @param bqlConfig A {@link BQLConfig}.
     */
    public QueryExtractor(BQLConfig bqlConfig) {
        queryMaxDuration = bqlConfig.getAs(BulletConfig.QUERY_MAX_DURATION, Long.class);
    }

    /**
     * Creates a {@link Query} from the given {@link ProcessedQuery}.
     *
     * @param processedQuery The query components to create a query from.
     * @return A {@link Query} created from the given {@link ProcessedQuery}.
     */
    public Query extractQuery(ProcessedQuery processedQuery) {
        Query query = new Query();

        extractAggregation(processedQuery, query);
        extractDuration(processedQuery, query, queryMaxDuration);
        extractFilter(processedQuery, query);
        extractProjection(processedQuery, query);
        extractPostAggregations(processedQuery, query);
        extractWindow(processedQuery, query);

        return query;
    }

    private static void extractAggregation(ProcessedQuery processedQuery, Query query) {
        query.setAggregation(AggregationExtractor.extractAggregation(processedQuery));
    }

    private static void extractDuration(ProcessedQuery processedQuery, Query query, Long queryMaxDuration) {
        if (processedQuery.getTimeDuration() != null) {
            query.setDuration(Math.min(processedQuery.getTimeDuration(), queryMaxDuration));
        }
    }

    private static void extractFilter(ProcessedQuery processedQuery, Query query) {
        if (processedQuery.getWhereNode() != null) {
            query.setFilter(processedQuery.getExpression(processedQuery.getWhereNode()));
        }
    }

    private static void extractProjection(ProcessedQuery processedQuery, Query query) {
        query.setProjection(ProjectionExtractor.extractProjection(processedQuery));
    }

    private static void extractPostAggregations(ProcessedQuery processedQuery, Query query) {
        query.setPostAggregations(PostAggregationExtractor.extractPostAggregations(processedQuery));
    }

    private static void extractWindow(ProcessedQuery processedQuery, Query query) {
        if (processedQuery.getWindow() != null) {
            query.setWindow(WindowExtractor.extractWindow(processedQuery));
        }
    }
}
