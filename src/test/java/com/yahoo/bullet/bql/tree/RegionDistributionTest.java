/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.aggregations.Distribution.Type.PMF;
import static com.yahoo.bullet.aggregations.Distribution.Type.QUANTILE;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class RegionDistributionTest {
    private List<Expression> columns;
    private RegionDistribution distribution;

    @BeforeClass
    public void setUP() {
        columns = singletonList(identifier("aaa"));
        distribution = new RegionDistribution(columns, QUANTILE, 0.0, 1.0, 0.2);
    }

    @Test
    public void testEquals() {
        RegionDistribution copy = distribution;
        assertTrue(distribution.equals(copy));
        assertFalse(distribution.equals(null));
        assertFalse(distribution.equals(columns));

        List<Expression> diffColumns = singletonList(identifier("bbb"));
        RegionDistribution distributionDiffColumns = new RegionDistribution(diffColumns, QUANTILE, 0.0, 1.0, 0.2);
        assertFalse(distribution.equals(distributionDiffColumns));

        RegionDistribution distributionDiffType = new RegionDistribution(columns, PMF, 0.0, 1.0, 0.2);
        assertFalse(distribution.equals(distributionDiffType));

        RegionDistribution distributionDiffStart = new RegionDistribution(columns, QUANTILE, 0.4, 1.0, 0.2);
        assertFalse(distribution.equals(distributionDiffStart));

        RegionDistribution distributionDiffEnd = new RegionDistribution(columns, QUANTILE, 0.0, 0.9, 0.2);
        assertFalse(distribution.equals(distributionDiffEnd));

        RegionDistribution distributionDiffIncrement = new RegionDistribution(columns, QUANTILE, 0.0, 1.0, 0.4);
        assertFalse(distribution.equals(distributionDiffIncrement));
    }
}
