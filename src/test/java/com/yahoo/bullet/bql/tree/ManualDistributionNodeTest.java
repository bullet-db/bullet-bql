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

public class ManualDistributionNodeTest {
    private ManualDistributionNode distribution;
    private List<ExpressionNode> columns;
    private List<Double> points;

    /*@BeforeClass
    public void setUp() {
        columns = singletonList(identifier("aaa"));
        points = ImmutableList.of(0.5, 1.0);
        distribution = new ManualDistributionNode(columns, QUANTILE, points);
    }

    @Test
    public void testEquals() {
        ManualDistributionNode copy = distribution;
        assertTrue(distribution.equals(copy));
        assertFalse(distribution.equals(null));
        assertFalse(distribution.equals(columns));

        List<ExpressionNode> diffColumns = singletonList(identifier("bbb"));
        ManualDistributionNode distributionDiffColumns = new ManualDistributionNode(diffColumns, QUANTILE, points);
        assertFalse(distribution.equals(distributionDiffColumns));

        ManualDistributionNode distributionDiffType = new ManualDistributionNode(columns, PMF, points);
        assertFalse(distribution.equals(distributionDiffType));

        List<Double> diffPoints = ImmutableList.of(0.4, 0.8);
        ManualDistributionNode distributionDiffPoints = new ManualDistributionNode(columns, QUANTILE, diffPoints);
        assertFalse(distribution.equals(distributionDiffPoints));
    }*/
}
