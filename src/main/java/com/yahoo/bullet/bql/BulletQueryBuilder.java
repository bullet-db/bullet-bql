/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.query.QueryProcessor;
import com.yahoo.bullet.bql.parser.BQLParser;
import com.yahoo.bullet.bql.query.QueryBuilder;
import com.yahoo.bullet.bql.tree.QueryNode;
import com.yahoo.bullet.bql.util.ExpressionFormatter;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.typesystem.Schema;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

@Slf4j
public class BulletQueryBuilder {
    private final BQLParser bqlParser = new BQLParser();
    private final BQLConfig config;
    private final Schema schema;
    private final int maxQueryLength;

    /**
     * Constructor that initializes a BulletQueryBuilder.
     *
     * @param bulletConfig A {@link BulletConfig} that will merge with {@link BQLConfig}.
     */
    public BulletQueryBuilder(BulletConfig bulletConfig) {
        config = new BQLConfig(bulletConfig);
        schema = config.getSchema();
        maxQueryLength = config.getAs(BQLConfig.BQL_MAX_QUERY_LENGTH, Integer.class);
    }

    /**
     * Build a Bullet {@link Query} from BQL string.
     *
     * @param bql The BQL String that contains a query.
     * @return A {@link BQLResult}.
     */
    public BQLResult buildQuery(String bql) {
        if (Utilities.isEmpty(bql)) {
            return makeBQLResultError("The given BQL query is empty.", "Please specify a non-empty query.");
        }
        if (bql.length() > maxQueryLength) {
            return makeBQLResultError("The given BQL string is too long. (" + bql.length() + " characters)",
                                      "Please reduce the length of the query to at most " + maxQueryLength + " characters.");
        }
        try {
            // Parse BQL into node tree
            QueryNode queryNode = bqlParser.createQueryNode(bql);

            // Parse node tree into query components
            ProcessedQuery processedQuery = QueryProcessor.visit(queryNode);
            if (!processedQuery.validate()) {
                return new BQLResult(processedQuery.getErrors());
            }

            QueryBuilder builder = new QueryBuilder(queryNode, processedQuery, schema);
            if (builder.hasErrors()) {
                return new BQLResult(builder.getErrors());
            }
            Query query = builder.getQuery();
            query.configure(config);

            return new BQLResult(query, ExpressionFormatter.format(queryNode, true));
        } catch (BulletException e) {
            return makeBQLResultError(e.getError());
        } catch (ParsingException e) {
            return makeBQLResultError(e.getMessage(), "This is a parsing error.");
        } catch (Exception e) {
            return makeBQLResultError(e.getMessage(), "This is an application error and not a user error.");
        }
    }

    private BQLResult makeBQLResultError(BulletError error) {
        return new BQLResult(Collections.singletonList(error));
    }

    private BQLResult makeBQLResultError(String error, String resolution) {
        return makeBQLResultError(new BulletError(error, resolution));
    }
}
