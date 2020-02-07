/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yahoo.bullet.bql.query.ClassifiedQuery;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.query.QueryClassifier;
import com.yahoo.bullet.bql.query.QueryProcessor;
import com.yahoo.bullet.bql.extractor.QueryExtractor;
import com.yahoo.bullet.bql.parser.BQLParser;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Query;

import java.util.List;
import java.util.Objects;

public class BulletQueryBuilder {
    private static final Gson GSON = new GsonBuilder().create();
    private final QueryProcessor queryProcessor = new QueryProcessor();
    private final BQLParser bqlParser = new BQLParser();
    private final QueryExtractor queryExtractor;
    private final BQLConfig config;

    /**
     * Constructor that initializes a BulletQueryBuilder.
     *
     * @param bulletConfig A {@link BulletConfig} that will merge with {@link BQLConfig}.
     */
    public BulletQueryBuilder(BulletConfig bulletConfig) {
        config = new BQLConfig(bulletConfig);
        queryExtractor = new QueryExtractor(new BQLConfig(config));
    }

    /**
     * Build a Bullet {@link Query} from BQL string.
     *
     * @param bql The BQL String that contains a query.
     * @return A {@link BQLResult}.
     */
    public BQLResult buildQuery(String bql) {
        Objects.requireNonNull(bql);

        // Parse BQL to node tree.
        QueryNode queryNode = bqlParser.createQueryNode(bql);

        // Process the query node into query components and validate components
        ProcessedQuery processedQuery = queryProcessor.process(queryNode);

        ClassifiedQuery classifiedQuery = QueryClassifier.classify(processedQuery);

        List<BulletError> errors = classifiedQuery.validate();
        if (!errors.isEmpty()) {
            return new BQLResult(errors);
        }

        //return new BQLResult(classifiedQuery.extractQuery(config));

        // Build the query
        return new BQLResult(queryExtractor.extractQuery(processedQuery));

        /*
        ProcessedQuery processedQuery = queryProcessor.process(queryNode);

        // Can let this part just fall through and have queryExtractor return null for query if there are errors in processedQuery
        if (!processedQuery.getErrors().isEmpty()) {
            return new BQLResult(processedQuery.getErrors());
        }

        Query query = queryExtractor.extractQuery(processedQuery);

        List<BulletError> errors = QueryValidator.validate(query, processedQuery, schema);
        List<BulletError> errors = new QueryValidator(processedQuery, query, schema).validate();
        if (!errors.isEmpty()) {
            return new BQLResult(errors);
        }

        return query;
        */
    }

    /**
     * Build a JSON from a {@link Query}.
     *
     * @param query A {@link Query}.
     * @return A JSON String.
     */
    public String toJson(Query query) {
        return GSON.toJson(query);
    }
}
