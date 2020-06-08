/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import com.yahoo.bullet.bql.extractor.AggregationExtractor;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.aggregations.TopK;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class TopKTest extends IntegrationTest {
    @Test
    public void testTopK() {
        build("SELECT TOP(10, abc) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKWithThreshold() {
        build("SELECT TOP(10, 100, abc) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getThreshold(), (Long) 100L);
        Assert.assertEquals(aggregation.getName(), AggregationExtractor.DEFAULT_TOP_K_NAME);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKMultipleNotAllowed() {
        build("SELECT TOP(10, abc), TOP(10, def) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Cannot have multiple top k.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testTopKAsValueNotAllowed() {
        build("SELECT TOP(10, abc) + 5 FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Top k cannot be treated as a value.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testTopKLimitNotAllowed() {
        build("SELECT TOP(10, abc) FROM STREAM() LIMIT 10");
        Assert.assertEquals(errors.get(0).getError(), "LIMIT clause is not supported for queries with top k.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testTopKOrderByNotAllowed() {
        build("SELECT TOP(10, abc) FROM STREAM() ORDER BY abc");
        Assert.assertEquals(errors.get(0).getError(), "ORDER BY clause is not supported for queries with top k.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testTopKWithMultipleFields() {
        build("SELECT TOP(10, abc, def) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc"));
        Assert.assertTrue(aggregation.getFields().contains("def"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "abc");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def"), "def");
        Assert.assertEquals(aggregation.getName(), AggregationExtractor.DEFAULT_TOP_K_NAME);
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKWithComputations() {
        build("SELECT TOP(10, abc, def + 5) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("def + 5", binary(field("def", Type.FLOAT),
                                                                                                  value(5),
                                                                                                  Operation.ADD,
                                                                                                  Type.FLOAT)));

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc"));
        Assert.assertTrue(aggregation.getFields().contains("def + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "abc");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def + 5"), "def + 5");
        Assert.assertEquals(aggregation.getName(), AggregationExtractor.DEFAULT_TOP_K_NAME);
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKWithAdditionalFields() {
        build("SELECT TOP(10, abc, def + 5), abc + 5, count + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("def + 5", binary(field("def", Type.FLOAT),
                                                                                                  value(5),
                                                                                                  Operation.ADD,
                                                                                                  Type.FLOAT)));

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc"));
        Assert.assertTrue(aggregation.getFields().contains("def + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "abc");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def + 5"), "def + 5");
        Assert.assertEquals(aggregation.getName(), AggregationExtractor.DEFAULT_TOP_K_NAME);
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields().size(), 2);
        Assert.assertEquals(computation.getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                        value(5),
                                                                                        Operation.ADD,
                                                                                        Type.INTEGER)));
        Assert.assertEquals(computation.getFields().get(1), new Field("count + 5", binary(field("count", Type.LONG),
                                                                                          value(5),
                                                                                          Operation.ADD,
                                                                                          Type.LONG)));
    }

    @Test
    public void testTopKAlias() {
        build("SELECT TOP(10, abc) AS top, top + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getName(), "top");
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields().size(), 1);
        Assert.assertEquals(computation.getFields().get(0), new Field("top + 5", binary(field("top", Type.LONG),
                                                                                        value(5),
                                                                                        Operation.ADD,
                                                                                        Type.LONG)));
    }

    @Test
    public void testTopKWithAlias() {
        build("SELECT TOP(10, abc, def), abc AS one, def AS two FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc"));
        Assert.assertTrue(aggregation.getFields().contains("def"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "one");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def"), "two");
        Assert.assertEquals(aggregation.getName(), AggregationExtractor.DEFAULT_TOP_K_NAME);
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }
}
