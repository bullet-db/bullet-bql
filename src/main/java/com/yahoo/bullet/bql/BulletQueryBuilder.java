/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yahoo.bullet.bql.classifier.ProcessedQuery;
import com.yahoo.bullet.bql.classifier.QueryProcessor;
import com.yahoo.bullet.bql.classifier.QueryClassifier;
import com.yahoo.bullet.bql.extractor.QueryExtractor;
import com.yahoo.bullet.bql.parser.BQLParser;
import com.yahoo.bullet.bql.parser.ParsingOptions;
import com.yahoo.bullet.bql.parser.ParsingOptions.DecimalLiteralTreatment;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Query;

import static java.util.Objects.requireNonNull;

public class BulletQueryBuilder {
    private static final Gson GSON = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
    private final QueryProcessor queryProcessor = new QueryProcessor();
    private final QueryClassifier queryClassifier = new QueryClassifier();
    private final BQLParser bqlParser;
    private final ParsingOptions parsingOptions;
    private final QueryExtractor queryExtractor;

    /**
     * Constructor that initializes a BulletQueryBuilder.
     *
     * @param config A {@link BulletConfig} that will merge with {@link BQLConfig}.
     */
    public BulletQueryBuilder(BulletConfig config) {
        bqlParser = new BQLParser();
        BQLConfig bqlConfig = new BQLConfig(requireNonNull(config));
        queryExtractor = new QueryExtractor(bqlConfig);

        // Default is treating decimal as double.
        String decimalLiteralTreatment = bqlConfig.getAs(BQLConfig.BQL_DECIMAL_LITERAL_TREATMENT, String.class);
        parsingOptions = new ParsingOptions(DecimalLiteralTreatment.valueOf(decimalLiteralTreatment));
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
        QueryNode queryNode = bqlParser.createQueryNode(bql, parsingOptions);

        // Process the query node into query components and validate components
        ProcessedQuery processedQuery = queryProcessor.process(queryNode);

        // TODO probably just push this to query processing/validating
        // Classify the query
        queryClassifier.classifyQuery(processedQuery);

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

    private String toJson(Query query) {
        return GSON.toJson(query);
    }
}
