/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.aggregations.DistributionType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class RegionDistributionNodeTest {
    private RegionDistributionNode node;

    @BeforeClass
    public void setup() {
        node = new RegionDistributionNode(DistributionType.QUANTILE, identifier("abc"), 0.0, 5.0, 1.0, null);
    }

    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new RegionDistributionNode(DistributionType.QUANTILE, identifier("abc"), 0.0, 5.0, 1.0, null),
                                        new RegionDistributionNode(DistributionType.PMF, identifier("abc"), 0.0, 5.0, 1.0, null),
                                        new RegionDistributionNode(DistributionType.QUANTILE, identifier("def"), 0.0, 5.0, 1.0, null),
                                        new RegionDistributionNode(DistributionType.QUANTILE, identifier("abc"), 1.0, 5.0, 1.0, null),
                                        new RegionDistributionNode(DistributionType.QUANTILE, identifier("abc"), 0.0, 10.0, 1.0, null),
                                        new RegionDistributionNode(DistributionType.QUANTILE, identifier("abc"), 0.0, 5.0, 2.0, null));
    }

    @Test
    public void testToString() {
        Assert.assertEquals(node.toString(), "QUANTILE(abc, REGION, 0.0, 5.0, 1.0)");
    }
}
