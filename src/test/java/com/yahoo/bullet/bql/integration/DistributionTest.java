/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.aggregations.LinearDistribution;
import com.yahoo.bullet.query.aggregations.ManualDistribution;
import com.yahoo.bullet.query.aggregations.RegionDistribution;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class DistributionTest extends IntegrationTest {
    @Test
    public void testQuantileDistribution() {
        build("SELECT QUANTILE(abc, LINEAR, 11) FROM STREAM() LIMIT 20");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        LinearDistribution aggregation = (LinearDistribution) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.DISTRIBUTION);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getDistributionType(), DistributionType.QUANTILE);
        Assert.assertEquals(aggregation.getNumberOfPoints(), 11);
        Assert.assertEquals(aggregation.getSize(), (Integer) 20);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testPMFDistribution() {
        build("SELECT FREQ(abc, REGION, 2000, 20000, 500) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        RegionDistribution aggregation = (RegionDistribution) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.DISTRIBUTION);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getDistributionType(), DistributionType.PMF);
        Assert.assertEquals(aggregation.getStart(), 2000.0);
        Assert.assertEquals(aggregation.getEnd(), 20000.0);
        Assert.assertEquals(aggregation.getIncrement(), 500.0);
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testCDFDistribution() {
        build("SELECT CUMFREQ(abc, MANUAL, 20000, 2000, 15000, 45000) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        ManualDistribution aggregation = (ManualDistribution) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.DISTRIBUTION);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getDistributionType(), DistributionType.CDF);
        // Points are sorted
        Assert.assertEquals(aggregation.getPoints(), Arrays.asList(2000.0, 15000.0, 20000.0, 45000.0));
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testDistributionMultipleNotAllowed() {
        build("SELECT FREQ(abc, REGION, 2000, 20000, 500), CUMFREQ(abc, MANUAL, 20000, 2000, 15000, 45000) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Cannot have multiple distribution functions.");
    }

    @Test
    public void testDistributionAsValueNotAllowed() {
        build("SELECT QUANTILE(abc, LINEAR, 11) + 5 FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Distribution functions cannot be treated as values.");
    }

    @Test
    public void testDistributionWithComputation() {
        build("SELECT QUANTILE(abc + 5, LINEAR, 11) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        LinearDistribution aggregation = (LinearDistribution) query.getAggregation();

        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getDistributionType(), DistributionType.QUANTILE);
        Assert.assertEquals(aggregation.getNumberOfPoints(), 11);
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertNull(query.getPostAggregations());
    }
}
