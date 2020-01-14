/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.aggregations.Distribution;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class RegionDistributionNodeTest extends ExpressionNodeTest {
    private RegionDistributionNode node;

    @BeforeClass
    public void setup() {
        node = new RegionDistributionNode(Distribution.Type.QUANTILE, identifier("abc"), 0.0, 5.0, 1.0);
    }

    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new RegionDistributionNode(Distribution.Type.QUANTILE, identifier("abc"), 0.0, 5.0, 1.0),
                              new RegionDistributionNode(Distribution.Type.PMF, identifier("abc"), 0.0, 5.0, 1.0),
                              new RegionDistributionNode(Distribution.Type.QUANTILE, identifier("def"), 0.0, 5.0, 1.0),
                              new RegionDistributionNode(Distribution.Type.QUANTILE, identifier("abc"), 1.0, 5.0, 1.0),
                              new RegionDistributionNode(Distribution.Type.QUANTILE, identifier("abc"), 0.0, 10.0, 1.0),
                              new RegionDistributionNode(Distribution.Type.QUANTILE, identifier("abc"), 0.0, 5.0, 2.0));
    }

    @Test
    public void testGetAttributes() {
        Map<String, Object> attributes = node.getAttributes();
        Assert.assertEquals(attributes.size(), 4);
        Assert.assertEquals(attributes.get(Distribution.TYPE), Distribution.Type.QUANTILE);
        Assert.assertEquals(attributes.get(Distribution.RANGE_START), 0.0);
        Assert.assertEquals(attributes.get(Distribution.RANGE_END), 5.0);
        Assert.assertEquals(attributes.get(Distribution.RANGE_INCREMENT), 1.0);
    }

    @Test
    public void testAttributesToString() {
        Assert.assertEquals(node.attributesToString(), "QUANTILE(abc, REGION, 0.0, 5.0, 1.0)");
    }
}
