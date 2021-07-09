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

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class ManualDistributionNodeTest {
    private ManualDistributionNode node;

    @BeforeClass
    public void setup() {
        node = new ManualDistributionNode(DistributionType.QUANTILE, identifier("abc"), Collections.singletonList(5.0), null);
    }

    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new ManualDistributionNode(DistributionType.QUANTILE, identifier("abc"), Collections.singletonList(5.0), null),
                                        new ManualDistributionNode(DistributionType.PMF, identifier("abc"), Collections.singletonList(5.0), null),
                                        new ManualDistributionNode(DistributionType.QUANTILE, identifier("def"), Collections.singletonList(5.0), null),
                                        new ManualDistributionNode(DistributionType.QUANTILE, identifier("abc"), Collections.singletonList(10.0), null));

    }

    @Test
    public void testToString() {
        Assert.assertEquals(node.toString(), "QUANTILE(abc, MANUAL, 5.0)");
    }
}
