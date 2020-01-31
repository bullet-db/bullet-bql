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

public class LinearDistributionNodeTest extends NodeTest {
    private LinearDistributionNode node;

    @BeforeClass
    public void setup() {
        node = new LinearDistributionNode(Distribution.Type.QUANTILE, identifier("abc"), 5L);
    }

    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new LinearDistributionNode(Distribution.Type.QUANTILE, identifier("abc"), 5L),
                              new LinearDistributionNode(Distribution.Type.PMF, identifier("abc"), 5L),
                              new LinearDistributionNode(Distribution.Type.QUANTILE, identifier("def"), 5L),
                              new LinearDistributionNode(Distribution.Type.QUANTILE, identifier("abc"), 10L));
    }

    @Test
    public void testGetAttributes() {
        Map<String, Object> attributes = node.getAttributes();
        Assert.assertEquals(attributes.size(), 2);
        Assert.assertEquals(attributes.get(Distribution.TYPE), Distribution.Type.QUANTILE);
        Assert.assertEquals(attributes.get(Distribution.NUMBER_OF_POINTS), 5L);
    }

    @Test
    public void testAttributesToString() {
        Assert.assertEquals(node.attributesToString(), "QUANTILE(abc, LINEAR, 5)");
    }
}
