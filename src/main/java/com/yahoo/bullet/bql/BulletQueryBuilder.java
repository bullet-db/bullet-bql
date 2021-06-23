/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.query.QueryError;
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

import java.io.Serializable;
import java.util.Collections;

@Slf4j
public class BulletQueryBuilder implements Serializable {
    private static final long serialVersionUID = -4892719761308177347L;

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
            return makeError(QueryError.EMPTY_QUERY.format());
        }
        if (bql.length() > maxQueryLength) {
            String resolution = "Please reduce the length of the query to at most " + maxQueryLength + " characters.";
            return makeError(QueryError.QUERY_TOO_LONG.formatWithResolution(resolution, bql.length()));
        }
        try {
            // Parse BQL into node tree
            QueryNode queryNode = bqlParser.createQueryNode(bql);

            // Parse node tree into query components
            ProcessedQuery processedQuery = QueryProcessor.visit(queryNode);
            if (!processedQuery.validate()) {
                return new BQLResult(processedQuery.getErrors());
            }

            QueryBuilder builder = new QueryBuilder(processedQuery, schema);
            if (builder.hasErrors()) {
                return new BQLResult(builder.getErrors());
            }
            Query query = builder.getQuery();
            query.configure(config);

            return new BQLResult(query, ExpressionFormatter.format(queryNode, true));
        } catch (BulletException e) {
            return makeError(e.getError());
        } catch (ParsingException e) {
            return makeError(QueryError.GENERIC_PARSING_ERROR.format(e.getMessage()));
        } catch (Exception e) {
            return makeError(QueryError.GENERIC_ERROR.format(e.getMessage()));
        }
    }

    private BQLResult makeError(BulletError error) {
        return new BQLResult(Collections.singletonList(error));
    }
}
