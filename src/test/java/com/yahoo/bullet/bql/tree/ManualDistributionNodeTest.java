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

import java.util.Collections;
import java.util.Map;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class ManualDistributionNodeTest extends NodeTest {
    private ManualDistributionNode node;

    @BeforeClass
    public void setup() {
        node = new ManualDistributionNode(Distribution.Type.QUANTILE, identifier("abc"), Collections.singletonList(5.0));
    }

    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new ManualDistributionNode(Distribution.Type.QUANTILE, identifier("abc"), Collections.singletonList(5.0)),
                              new ManualDistributionNode(Distribution.Type.PMF, identifier("abc"), Collections.singletonList(5.0)),
                              new ManualDistributionNode(Distribution.Type.QUANTILE, identifier("def"), Collections.singletonList(5.0)),
                              new ManualDistributionNode(Distribution.Type.QUANTILE, identifier("abc"), Collections.singletonList(10.0)));

    }

    @Test
    public void testGetAttributes() {
        Map<String, Object> attributes = node.getAttributes();
        Assert.assertEquals(attributes.size(), 2);
        Assert.assertEquals(attributes.get(Distribution.TYPE), Distribution.Type.QUANTILE);
        Assert.assertEquals(attributes.get(Distribution.POINTS), Collections.singletonList(5.0));
    }

    @Test
    public void testAttributesToString() {
        Assert.assertEquals(node.attributesToString(), "QUANTILE(abc, MANUAL, 5.0)");
    }
}
