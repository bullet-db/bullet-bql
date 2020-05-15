/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.query.QueryProcessor;
import com.yahoo.bullet.bql.extractor.QueryExtractor;
import com.yahoo.bullet.bql.parser.BQLParser;
import com.yahoo.bullet.bql.query.QueryValidator;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.typesystem.Schema;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Slf4j
public class BulletQueryBuilder {
    private static final Gson GSON = new GsonBuilder().create();
    private final QueryProcessor queryProcessor = new QueryProcessor();
    private final BQLParser bqlParser = new BQLParser();
    private final BQLConfig config;
    private final Schema schema;

    /**
     * Constructor that initializes a BulletQueryBuilder.
     *
     * @param bulletConfig A {@link BulletConfig} that will merge with {@link BQLConfig}.
     */
    public BulletQueryBuilder(BulletConfig bulletConfig) {
        config = new BQLConfig(bulletConfig);
        schema = config.getSchema();
    }

    /**
     * Build a Bullet {@link Query} from BQL string.
     *
     * @param bql The BQL String that contains a query.
     * @return A {@link BQLResult}.
     */
    public BQLResult buildQuery(String bql) {
        try {
            Objects.requireNonNull(bql);

            // Parse BQL to node tree.
            QueryNode queryNode = bqlParser.createQueryNode(bql);

            ProcessedQuery processedQuery = queryProcessor.process(queryNode);

            if (!processedQuery.getErrors().isEmpty()) {
                return new BQLResult(processedQuery.getErrors());
            }

            Query query = QueryExtractor.extractQuery(processedQuery);

            query.configure(config);

            List<BulletError> errors = QueryValidator.validate(processedQuery, query, schema);
            if (!errors.isEmpty()) {
                return new BQLResult(errors);
            }
            return new BQLResult(query);
        } catch (BulletException e) {
            return null;
        } catch (NullPointerException e) {
            return makeBQLResultError(e.getMessage(), "This is most likely an application error and not a user error.");
        } catch (ParsingException e) {
            return makeBQLResultError(e.getMessage(), "This is a parsing exception.");
        } catch (Exception e) {
            return makeBQLResultError(e.getMessage(), "This is most likely an application error and not a user error.");
        }
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

    private BQLResult makeBQLResultError(String error, String resolution) {
        return new BQLResult(Collections.singletonList(new BulletError(error, resolution)));
    }
}
