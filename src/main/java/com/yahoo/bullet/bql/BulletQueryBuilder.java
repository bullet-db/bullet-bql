/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yahoo.bullet.bql.classifier.QueryClassifier;
import com.yahoo.bullet.bql.classifier.QueryClassifier.QueryType;
import com.yahoo.bullet.bql.extractor.QueryExtractor;
import com.yahoo.bullet.bql.parser.BQLParser;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.parser.ParsingOptions;
import com.yahoo.bullet.bql.parser.ParsingOptions.DecimalLiteralTreatment;
import com.yahoo.bullet.bql.parser.StatementSplitter;
import com.yahoo.bullet.bql.tree.Statement;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Query;

import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;

public class BulletQueryBuilder {
    private static final Gson GSON = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
    private final QueryClassifier queryClassifier = new QueryClassifier();

    private final BQLParser bqlParser;
    private final String delimiter;
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

        // Default is ";".
        delimiter = bqlConfig.getAs(BQLConfig.BQL_DELIMITER, String.class);

        // Default is treating decimal as double.
        String decimalLiteralTreatment = bqlConfig.getAs(BQLConfig.BQL_DECIMAL_LITERAL_TREATMENT, String.class);
        parsingOptions = new ParsingOptions(DecimalLiteralTreatment.valueOf(decimalLiteralTreatment));
    }

    /**
     * Build a Bullet {@link Query} from BQL string.
     *
     * @param bql The BQL String that contains a query.
     * @return A Bullet {@link Query}.
     * @throws ParsingException              when no BQL or more than one BQL is provided.
     * @throws NullPointerException          when bql is null.
     * @throws IllegalArgumentException      when bql argument is not valid.
     * @throws UnsupportedOperationException when bql operation is not valid.
     * @throws AssertionError                when DecimalLiteralTreatment is not valid.
     */
    public Query buildQuery(String bql) throws ParsingException, NullPointerException, IllegalArgumentException, UnsupportedOperationException, AssertionError {
        bql = normalizeBQL(requireNonNull(bql));

        // Parse BQL to node tree.
        Statement statement = bqlParser.createStatement(bql, parsingOptions);

        // Get the QueryType of BQL from node tree.
        QueryType type = queryClassifier.classifyQuery(statement);

        // Parse node tree to Bullet Query.
        return queryExtractor.validateAndExtract(statement, type);
    }

    /**
     * Build a Bullet JSON from BQL string.
     *
     * @param bql The BQL String that contains query.
     * @return A Bullet JSON String.
     * @throws ParsingException              when no BQL or more than one BQL is provided.
     * @throws NullPointerException          when bql is null.
     * @throws IllegalArgumentException      when bql argument is not valid.
     * @throws UnsupportedOperationException when bql operation is not valid.
     * @throws AssertionError                when DecimalLiteralTreatment is not valid.
     */
    public String buildJson(String bql) throws ParsingException, NullPointerException, IllegalArgumentException, UnsupportedOperationException, AssertionError {
        return toJson(buildQuery(bql));
    }

    private String normalizeBQL(String bql) throws ParsingException {
        if (!bql.contains(delimiter)) {
            return bql;
        }

        StatementSplitter splitter = new StatementSplitter(bql, singleton(delimiter));
        // If there are 0 or more than 1 BQL statements in input string, throw exception.
        if (splitter.getCompleteStatements().size() != 1) {
            throw new ParsingException("Please provide only 1 valid BQL statement");
        } else {
            return splitter.getCompleteStatements().get(0).statement();
        }
    }

    private String toJson(Query query) {
        return GSON.toJson(query);
    }
}
