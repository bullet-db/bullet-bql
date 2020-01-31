/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yahoo.bullet.bql.processor.ProcessedQuery;
import com.yahoo.bullet.bql.processor.QueryProcessor;
import com.yahoo.bullet.bql.extractor.QueryExtractor;
import com.yahoo.bullet.bql.parser.BQLParser;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.parsing.Query;

import java.util.Objects;

public class BulletQueryBuilder {
    private static final Gson GSON = new GsonBuilder().create();
    private final QueryProcessor queryProcessor = new QueryProcessor();
    private final BQLParser bqlParser = new BQLParser();
    private final QueryExtractor queryExtractor;

    /**
     * Constructor that initializes a BulletQueryBuilder.
     *
     * @param config A {@link BQLConfig}.
     */
    public BulletQueryBuilder(BQLConfig config) {
        queryExtractor = new QueryExtractor(config);
    }

    /**
     * Build a Bullet {@link Query} from BQL string.
     *
     * @param bql The BQL String that contains a query.
     * @return A Bullet {@link Query}.
     */
    public Query buildQuery(String bql) {
        Objects.requireNonNull(bql);

        // Parse BQL to node tree.
        QueryNode node = bqlParser.createQueryNode(bql);

        // Process the query node into query components and validate components
        ProcessedQuery processedQuery = queryProcessor.process(node);

        // Could separately validate

        // Type-checking expression nodes? type-checking the expressions

        // Need schema

        // TODO Could have processedQuery#getErrors()
        // Would have to throw from here to get the errors...


        // Build the query
        return queryExtractor.extractQuery(processedQuery);
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
