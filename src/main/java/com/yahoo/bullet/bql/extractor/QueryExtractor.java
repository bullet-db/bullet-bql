/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.processor.ProcessedQuery;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.Window;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import static com.yahoo.bullet.parsing.Window.EMIT_EVERY_FIELD;
import static com.yahoo.bullet.parsing.Window.INCLUDE_FIRST_FIELD;
import static com.yahoo.bullet.parsing.Window.TYPE_FIELD;

@Slf4j
public class QueryExtractor {
    private final AggregationExtractor aggregationExtractor = new AggregationExtractor();
    private final ProjectionExtractor projectionExtractor = new ProjectionExtractor();
    private final PostAggregationExtractor postAggregationExtractor = new PostAggregationExtractor();
    private final Long queryMaxDuration;

    /**
     * The constructor with a {@link BulletConfig}.
     *
     * @param bulletConfig A {@link BulletConfig}.
     */
    public QueryExtractor(BulletConfig bulletConfig) {
        queryMaxDuration = bulletConfig.getAs(BulletConfig.QUERY_MAX_DURATION, Long.class);
    }

    /**
     * Return a {@link Query}.
     *
     * @param processedQuery .
     * @return A {@link Query}.
     */
    public Query extractQuery(ProcessedQuery processedQuery) {
        Query query = new Query();

        extractAggregation(processedQuery, query);
        extractDuration(processedQuery, query);
        extractFilter(processedQuery, query);
        extractProjection(processedQuery, query);
        extractPostAggregations(processedQuery, query);
        extractWindow(processedQuery, query);

        return query;
    }

    private void extractAggregation(ProcessedQuery processedQuery, Query query) {
        query.setAggregation(aggregationExtractor.extractAggregation(processedQuery));
    }

    private void extractDuration(ProcessedQuery processedQuery, Query query) {
        if (processedQuery.getTimeDuration() != null) {
            query.setDuration(Math.min(processedQuery.getTimeDuration(), queryMaxDuration));
        }
    }

    private void extractFilter(ProcessedQuery processedQuery, Query query) {
        if (processedQuery.getWhereNode() != null) {
            query.setFilter(processedQuery.getExpression(processedQuery.getWhereNode()));
        }
    }

    private void extractProjection(ProcessedQuery processedQuery, Query query) {
        query.setProjection(projectionExtractor.extractProjection(processedQuery));
    }

    private void extractPostAggregations(ProcessedQuery processedQuery, Query query) {
        query.setPostAggregations(postAggregationExtractor.extractPostAggregations(processedQuery));
    }

    private void extractWindow(ProcessedQuery processedQuery, Query query) {
        if (!processedQuery.isWindowed()) {
            return;
        }

        Window window = new Window();

        Map<String, Object> emit = new HashMap<>();
        emit.put(EMIT_EVERY_FIELD, processedQuery.getEmitEvery());
        emit.put(TYPE_FIELD, processedQuery.getEmitType().toString());
        window.setEmit(emit);

        if (processedQuery.getIncludeUnit() != null) {
            Map<String, Object> include = new HashMap<>();
            include.put(TYPE_FIELD, processedQuery.getIncludeUnit().toString());
            include.put(INCLUDE_FIRST_FIELD, processedQuery.getFirst());
            window.setInclude(include);
        }

        query.setWindow(window);
    }
}
