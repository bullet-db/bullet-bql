/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.aggregations.CountDistinct;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class CountDistinctTest extends IntegrationTest {
     @Test
    public void testCountDistinct() {
        build("SELECT COUNT(DISTINCT abc) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        CountDistinct aggregation = (CountDistinct) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.COUNT_DISTINCT);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getName(), "COUNT(DISTINCT abc)");
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testCountDistinctNonPrimitive() {
        build("SELECT COUNT(DISTINCT aaa, bbb) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The types of the arguments in COUNT(DISTINCT aaa, bbb) must be primitive. Types given: [STRING_MAP_LIST, STRING_MAP_MAP]");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testCountDistinctWithComputation() {
        build("SELECT COUNT(DISTINCT abc + 5) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        CountDistinct aggregation = (CountDistinct) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.COUNT_DISTINCT);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getName(), "COUNT(DISTINCT abc + 5)");
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testCountDistinctAsComputation() {
        build("SELECT COUNT(DISTINCT abc), COUNT(DISTINCT abc) + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        CountDistinct aggregation = (CountDistinct) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.COUNT_DISTINCT);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getName(), "COUNT(DISTINCT abc)");
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("COUNT(DISTINCT abc) + 5", binary(field("COUNT(DISTINCT abc)", Type.LONG),
                                                                                                                           value(5),
                                                                                                                           Operation.ADD,
                                                                                                                           Type.LONG))));
    }

    @Test
    public void testCountDistinctAsComputationOnly() {
        build("SELECT COUNT(DISTINCT abc) + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        CountDistinct aggregation = (CountDistinct) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.COUNT_DISTINCT);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getName(), "COUNT(DISTINCT abc)");
        Assert.assertEquals(query.getPostAggregations().size(), 2);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("COUNT(DISTINCT abc) + 5", binary(field("COUNT(DISTINCT abc)", Type.LONG),
                                                                                                                           value(5),
                                                                                                                           Operation.ADD,
                                                                                                                           Type.LONG))));

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("COUNT(DISTINCT abc)"));
    }

    @Test
    public void testCountDistinctAliasAsComputation() {
        build("SELECT COUNT(DISTINCT abc) AS count, COUNT(DISTINCT abc) + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        CountDistinct aggregation = (CountDistinct) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.COUNT_DISTINCT);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getName(), "count");
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("COUNT(DISTINCT abc) + 5", binary(field("count", Type.LONG),
                                                                                                                           value(5),
                                                                                                                           Operation.ADD,
                                                                                                                           Type.LONG))));
    }

    @Test
    public void testCountDistinctMultipleNotAllowed() {
        build("SELECT COUNT(DISTINCT abc), COUNT(DISTINCT def) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Cannot have multiple count distincts.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testCountDistinctLimitNotAllowed() {
        build("SELECT COUNT(DISTINCT abc) FROM STREAM() LIMIT 10");
        Assert.assertEquals(errors.get(0).getError(), "LIMIT clause is not supported for queries with count distinct.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testCountDistinctOrderByNotAllowed() {
        build("SELECT COUNT(DISTINCT abc) FROM STREAM() ORDER BY COUNT(DISTINCT abc)");
        Assert.assertEquals(errors.get(0).getError(), "ORDER BY clause is not supported for queries with count distinct.");
        Assert.assertEquals(errors.size(), 1);
    }
}
