/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.aggregations.Distribution.Type.PMF;
import static com.yahoo.bullet.aggregations.Distribution.Type.QUANTILE;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ManualDistributionTest {
    private ManualDistribution distribution;
    private List<Expression> columns;
    private List<Double> points;

    @BeforeClass
    public void setUp() {
        columns = singletonList(identifier("aaa"));
        points = ImmutableList.of(0.5, 1.0);
        distribution = new ManualDistribution(columns, QUANTILE, points);
    }

    @Test
    public void testEquals() {
        ManualDistribution copy = distribution;
        assertTrue(distribution.equals(copy));
        assertFalse(distribution.equals(null));
        assertFalse(distribution.equals(columns));

        List<Expression> diffColumns = singletonList(identifier("bbb"));
        ManualDistribution distributionDiffColumns = new ManualDistribution(diffColumns, QUANTILE, points);
        assertFalse(distribution.equals(distributionDiffColumns));

        ManualDistribution distributionDiffType = new ManualDistribution(columns, PMF, points);
        assertFalse(distribution.equals(distributionDiffType));

        List<Double> diffPoints = ImmutableList.of(0.4, 0.8);
        ManualDistribution distributionDiffPoints = new ManualDistribution(columns, QUANTILE, diffPoints);
        assertFalse(distribution.equals(distributionDiffPoints));
    }
}
