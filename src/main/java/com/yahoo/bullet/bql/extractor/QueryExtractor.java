/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.BQLConfig;
import com.yahoo.bullet.bql.classifier.ProcessedQuery;
import com.yahoo.bullet.bql.tree.SortItemNode;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Culling;
import com.yahoo.bullet.parsing.Having;
import com.yahoo.bullet.parsing.OrderBy;
import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.Window;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.yahoo.bullet.bql.classifier.ProcessedQuery.QueryType;
import static com.yahoo.bullet.parsing.Window.EMIT_EVERY_FIELD;
import static com.yahoo.bullet.parsing.Window.INCLUDE_FIRST_FIELD;
import static com.yahoo.bullet.parsing.Window.TYPE_FIELD;

@Slf4j
public class QueryExtractor {
    private final AggregationExtractor aggregationExtractor = new AggregationExtractor();
    private final ProjectionExtractor projectionExtractor = new ProjectionExtractor();
    private final ComputationExtractor computationExtractor = new ComputationExtractor();
    private final TransientFieldExtractor transientFieldExtractor = new TransientFieldExtractor();
    private final Long queryMaxDuration;

    /**
     * The constructor with a {@link BQLConfig}.
     *
     * @param bqlConfig A {@link BQLConfig} for parsing BQL statement.
     */
    public QueryExtractor(BQLConfig bqlConfig) {
        queryMaxDuration = bqlConfig.getAs(BulletConfig.QUERY_MAX_DURATION, Long.class);
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
        if (processedQuery.getWhereNode() == null) {
            return;
        }
        query.setFilter(processedQuery.getExpression(processedQuery.getWhereNode()));
    }

    private void extractProjection(ProcessedQuery processedQuery, Query query) {
        query.setProjection(projectionExtractor.extractProjection(processedQuery));
    }

    private void extractPostAggregations(ProcessedQuery processedQuery, Query query) {
        List<PostAggregation> postAggregations = new ArrayList<>();
        extractHaving(processedQuery, postAggregations);
        extractComputations(processedQuery, postAggregations);
        extractOrderBy(processedQuery, postAggregations);
        extractTransientFields(processedQuery, postAggregations);
        if (!postAggregations.isEmpty()) {
            query.setPostAggregations(postAggregations);
        }
    }

    private void extractHaving(ProcessedQuery processedQuery, List<PostAggregation> postAggregations) {
        if (processedQuery.getHavingNode() == null) {
            return;
        }
        // Special K has a HAVING clause, but it's subsumed by Top K
        if (processedQuery.getQueryType() == QueryType.SPECIAL_K) {
            return;
        }
        postAggregations.add(new Having(processedQuery.getExpression(processedQuery.getHavingNode())));
    }

    private void extractComputations(ProcessedQuery processedQuery, List<PostAggregation> postAggregations) {
        Computation computation = computationExtractor.extractComputation(processedQuery);
        if (computation != null && !computation.getProjection().getFields().isEmpty()) {
            postAggregations.add(computation);
        }
    }

    private void extractOrderBy(ProcessedQuery processedQuery, List<PostAggregation> postAggregations) {
        if (processedQuery.getOrderByNodes().isEmpty()) {
            return;
        }
        List<OrderBy.SortItem> fields = processedQuery.getOrderByNodes().stream().map(node -> {
            OrderBy.SortItem sortItem = new OrderBy.SortItem();
            sortItem.setField(processedQuery.getAliasOrName(node.getExpression()));
            if (node.getOrdering() == SortItemNode.Ordering.DESCENDING) {
                sortItem.setDirection(OrderBy.Direction.DESC);
            } else {
                sortItem.setDirection(OrderBy.Direction.ASC);
            }
            return sortItem;
        }).collect(Collectors.toList());
        postAggregations.add(new OrderBy(fields));
    }

    private void extractTransientFields(ProcessedQuery processedQuery, List<PostAggregation> postAggregations) {
        Set<String> transientFields = transientFieldExtractor.extractTransientFields(processedQuery);
        if (transientFields != null && !transientFields.isEmpty()) {
            postAggregations.add(new Culling(transientFields));
        }
    }

    private void extractWindow(ProcessedQuery processedQuery, Query query) {
        if (!processedQuery.isWindowed()) {
            return;
        }
        Map<String, Object> emit = new HashMap<>();
        emit.put(EMIT_EVERY_FIELD, processedQuery.getEmitEvery());
        emit.put(TYPE_FIELD, processedQuery.getEmitType());

        Map<String, Object> include = new HashMap<>();
        include.put(TYPE_FIELD, processedQuery.getIncludeUnit());
        include.put(INCLUDE_FIRST_FIELD, processedQuery.getFirst());

        query.setWindow(new Window(emit, include));
    }
}
