/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.BQLConfig;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.aggregations.AggregationType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;

public class LazyPubSubMessageSerDeTest {
    @Test
    public void testLazyMessage() {
        LazyPubSubMessageSerDe serDe = new LazyPubSubMessageSerDe(new BQLConfig());
        PubSubMessage message = serDe.toMessage("foo", null, "bar");
        Assert.assertEquals(message.getId(), "foo");
        Assert.assertEquals(message.getContent(), "bar");
        Assert.assertEquals(message.getContentAsString(), "bar");
        Metadata metadata = message.getMetadata();
        Assert.assertEquals(metadata.getSignal(), Metadata.Signal.CUSTOM);
        Assert.assertNull(metadata.getContent());
        Assert.assertTrue(metadata.getCreated() <= System.currentTimeMillis());

        PubSubMessage unchanged = serDe.toMessage(message);
        Assert.assertEquals(unchanged.getId(), "foo");
        Assert.assertEquals(unchanged.getContent(), "bar");
        Assert.assertEquals(unchanged.getContentAsString(), "bar");
        metadata = message.getMetadata();
        Assert.assertEquals(metadata.getSignal(), Metadata.Signal.CUSTOM);
        Assert.assertNull(metadata.getContent());
        Assert.assertEquals(metadata.getCreated(), message.getMetadata().getCreated());
    }

    @Test
    public void testIgnoringOtherMessages() {
        LazyPubSubMessageSerDe serDe = new LazyPubSubMessageSerDe(new BQLConfig());
        PubSubMessage result = serDe.fromMessage(new PubSubMessage("id", new HashMap<>(), (Metadata) null));

        Assert.assertEquals(result.getId(), "id");
        Assert.assertNull(result.getMetadata());

        result = serDe.fromMessage(new PubSubMessage("id", new HashMap<>(), new Metadata(Metadata.Signal.KILL, null)));
        Assert.assertEquals(result.getId(), "id");
        Metadata metadata = result.getMetadata();
        Assert.assertEquals(metadata.getSignal(), Metadata.Signal.KILL);
        Assert.assertNull(metadata.getContent());
    }

    @Test
    public void testQueryCreation() {
        BQLConfig config = new BQLConfig();
        config.set(BulletConfig.AGGREGATION_MAX_SIZE, 5);
        LazyPubSubMessageSerDe serDe = new LazyPubSubMessageSerDe(config);

        PubSubMessage message = serDe.toMessage("id", null, "SELECT * FROM STREAM(MAX, TIME) LIMIT 100");
        long created = message.getMetadata().getCreated();
        PubSubMessage result = serDe.fromMessage(message);

        Assert.assertEquals(result.getId(), "id");
        Query actual = result.getContentAsQuery();
        Assert.assertEquals(actual.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertEquals(actual.getAggregation().getType(), AggregationType.RAW);
        Assert.assertEquals((long) actual.getAggregation().getSize(), 5L);
        Assert.assertEquals((long) actual.getDuration(), Long.MAX_VALUE);
        Metadata metadata = result.getMetadata();
        Assert.assertNull(metadata.getSignal());
        Assert.assertEquals(metadata.getContent(), "SELECT * FROM STREAM(MAX, TIME) LIMIT 100");
        Assert.assertEquals(metadata.getCreated(), created);

        Assert.assertNotSame(message, result);
        Assert.assertNotSame(message.getMetadata(), metadata);
        Assert.assertEquals(message.getMetadata().getCreated(), metadata.getCreated());
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testInvalidQueryCreation() {
        LazyPubSubMessageSerDe serDe = new LazyPubSubMessageSerDe(new BQLConfig());
        serDe.fromMessage(serDe.toMessage("", null, "garbage"));
    }
}
