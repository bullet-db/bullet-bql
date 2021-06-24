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

public class LinearDistributionNodeTest {
    private LinearDistributionNode node;

    @BeforeClass
    public void setup() {
        node = new LinearDistributionNode(DistributionType.QUANTILE, identifier("abc"), 5, null);
    }

    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new LinearDistributionNode(DistributionType.QUANTILE, identifier("abc"), 5, null),
                                        new LinearDistributionNode(DistributionType.PMF, identifier("abc"), 5, null),
                                        new LinearDistributionNode(DistributionType.QUANTILE, identifier("def"), 5, null),
                                        new LinearDistributionNode(DistributionType.QUANTILE, identifier("abc"), 10, null));
    }

    @Test
    public void testToString() {
        Assert.assertEquals(node.toString(), "QUANTILE(abc, LINEAR, 5)");
    }
}
