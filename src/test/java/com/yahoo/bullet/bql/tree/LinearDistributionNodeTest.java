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
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class LinearDistributionNodeTest {
    List<ExpressionNode> columns;
    private LinearDistributionNode distribution;

    @BeforeClass
    public void setUp() {
        columns = singletonList(identifier("aaa"));
        distribution = new LinearDistributionNode(columns, QUANTILE, (long) 10);
    }

    @Test
    public void testGetType() {
        assertEquals(distribution.getType(), QUANTILE);
    }

    @Test
    public void testGetChildren() {
        assertEquals(distribution.getChildren(), emptyList());
    }

    @Test
    public void testEquals() {
        LinearDistributionNode copy = distribution;
        assertTrue(distribution.equals(copy));
        assertFalse(distribution.equals(null));
        assertFalse(distribution.equals(columns));

        List<ExpressionNode> diffColumns = singletonList(identifier("bbb"));
        LinearDistributionNode distributionDiffColumns = new LinearDistributionNode(diffColumns, QUANTILE, (long) 10);
        assertFalse(distribution.equals(distributionDiffColumns));

        LinearDistributionNode distributionDiffType = new LinearDistributionNode(columns, PMF, (long) 10);
        assertFalse(distribution.equals(distributionDiffType));

        LinearDistributionNode distributionDiffNumberOfPoints = new LinearDistributionNode(columns, QUANTILE, (long) 20);
        assertFalse(distribution.equals(distributionDiffNumberOfPoints));
    }
}
