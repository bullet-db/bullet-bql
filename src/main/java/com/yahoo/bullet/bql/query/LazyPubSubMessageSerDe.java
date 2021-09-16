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
 * The {@link BulletConfig} provided to this class is used to configure the {@link BulletQueryBuilder} and the created
 * {@link Query} object.
 *
 * The BQL query string is stored as the payload in the {@link PubSubMessage} and it is not set in the message
 * {@link Metadata}. It is added back to the {@link Metadata} in {@link #fromMessage(PubSubMessage)}} and the query
 * string is converted back to a {@link Query} using the {@link #toQuery(String)} method and configured with the
 * provided {@link BulletConfig}.
 *
 * The {@link #toMessage(PubSubMessage)} does nothing by default, so you can safely use it for sending signals and other
 * messages or even regular query {@link PubSubMessage}! Do note that this SerDe identifies its lazy Query messages by
 * placing a {@link Metadata.Signal#CUSTOM} in its metadata. This is what it uses to process its messages in the
 * {@link #fromMessage(PubSubMessage)}. You will want to not send other messages with that signal.
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
        return toMessage(new PubSubMessage(id, queryString, new Metadata(Metadata.Signal.CUSTOM, null)));
    }

    @Override
    public PubSubMessage toMessage(PubSubMessage message) {
        return message;
    }

    @Override
    public PubSubMessage fromMessage(PubSubMessage message) {
        if (!message.hasSignal(Metadata.Signal.CUSTOM)) {
            return message;
        }
        String queryString = message.getContentAsString();
        Query query = toQuery(queryString);
        Metadata original = message.getMetadata();
        // Need to copy it if it's a custom Metadata with other fields besides the standard metadata fields
        Metadata meta = original.copy();
        meta.setSignal(null);
        meta.setContent(queryString);
        meta.setCreated(original.getCreated());
        return new PubSubMessage(message.getId(), query, meta);
    }

    /**
     * Convert the given String query back to a {@link Query} object. Override if you stored something else instead of
     * BQL.
     *
     * @param queryString The query.
     * @return A valid {@link Query} object.
     */
    protected Query toQuery(String queryString) {
        BQLResult result = queryBuilder.buildQuery(queryString);
        if (result.hasErrors()) {
            throw new RuntimeException("The SerDe does not handle invalid BQL!");
        }
        Query query = result.getQuery();
        query.configure(config);
        return query;
    }
}
