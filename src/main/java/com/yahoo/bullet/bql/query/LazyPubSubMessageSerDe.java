/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.BQLResult;
import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.PubSubMessageSerDe;
import com.yahoo.bullet.query.Query;

/**
 * This {@link PubSubMessageSerDe} is to be used to create a {@link PubSubMessage} for a query without actually creating
 * the {@link Query} object. The {@link Query} is instead created upon use of the {@link #fromMessage(PubSubMessage)}.
 * The {@link BulletConfig} provided to this class is used to configure the {@link BulletQueryBuilder}. The BQL query
 * string is stored as the payload in the {@link PubSubMessage} and it is explicitly not set in the message
 * {@link Metadata}. It is added back to the {@link Metadata} in {@link #fromMessage(PubSubMessage)}} and the query
 * string is converted back to a {@link Query} using the {@link #toQuery(String)} method. The
 * {@link #toMessage(PubSubMessage)} does nothing by default.
 *
 * Note, it is essential that the BQL query provided be valid if using the default BQL conversion in
 * {@link #toQuery(String)}! Otherwise, {@link #fromMessage(PubSubMessage)} will throw a {@link RuntimeException} when
 * it cannot create a {@link Query} object from the BQL query string.
 */
public class LazyPubSubMessageSerDe extends PubSubMessageSerDe {
    private static final long serialVersionUID = -6866473821452218792L;

    private BulletQueryBuilder queryBuilder;

    /**
     * Constructor.
     *
     * @param config The {@link BulletConfig} to configure this class and the {@link BulletQueryBuilder}.
     */
    public LazyPubSubMessageSerDe(BulletConfig config) {
        super(config);
        queryBuilder = new BulletQueryBuilder(config);
    }

    @Override
    public PubSubMessage toMessage(String id, Query query, String queryString) {
        return toMessage(new PubSubMessage(id, queryString, new Metadata(null, null)));
    }

    @Override
    public PubSubMessage toMessage(PubSubMessage message) {
        return message;
    }

    @Override
    public PubSubMessage fromMessage(PubSubMessage message) {
        String queryString = message.getContentAsString();
        Query query = toQuery(queryString);
        Metadata metadata = message.getMetadata();
        metadata.setContent(queryString);
        return new PubSubMessage(message.getId(), query, metadata);
    }

    /**
     * Convert the given String query back to a {@link Query} object. Override if you stored something else instead of
     * BQL.
     *
     * @param query The query.
     * @return A valid {@link Query} object.
     */
    protected Query toQuery(String query) {
        BQLResult result = queryBuilder.buildQuery(query);
        if (result.hasErrors()) {
            throw new RuntimeException("The SerDe does not handle invalid BQL!");
        }
        return result.getQuery();
    }
}
