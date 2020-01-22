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
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Query;

import static java.util.Objects.requireNonNull;

public class BulletQueryBuilder {
    private static final Gson GSON = new GsonBuilder().create();
    private final QueryProcessor queryProcessor = new QueryProcessor();
    private final BQLParser bqlParser = new BQLParser();
    private final QueryExtractor queryExtractor;

    /**
     * Constructor that initializes a BulletQueryBuilder.
     *
     * @param config A {@link BulletConfig}.
     */
    public BulletQueryBuilder(BulletConfig config) {
        queryExtractor = new QueryExtractor(config);
    }

    /**
     * Build a Bullet {@link Query} from BQL string.
     *
     * @param bql The BQL String that contains a query.
     * @return A Bullet {@link Query}.
     */
    public Query buildQuery(String bql) {
        requireNonNull(bql);

        // Parse BQL to node tree.
        QueryNode node = bqlParser.createQueryNode(bql);

        // TODO debugging
        //System.out.println(ExpressionFormatter.format(queryNode));

        // Process the query node into query components and validate components
        ProcessedQuery processedQuery = queryProcessor.process(node);

        // Build the query
        return queryExtractor.extractQuery(processedQuery);
    }

    /**
     * Build a Bullet JSON from BQL string.
     *
     * @param bql The BQL String that contains query.
     * @return A Bullet JSON String.
     * @throws NullPointerException          when bql is null.
     * @throws IllegalArgumentException      when bql argument is not valid.
     * @throws UnsupportedOperationException when bql operation is not valid.
     * @throws AssertionError                when DecimalLiteralTreatment is not valid.
     */
    public String buildJson(String bql) {
        return toJson(buildQuery(bql));
    }

    public String toJson(Query query) {
        return GSON.toJson(query);
    }
}
