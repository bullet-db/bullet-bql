/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.aggregations.Distribution;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;

public class DistributionNodeTest {
    private static class MockDistributionNode extends DistributionNode {
        MockDistributionNode(Distribution.Type type) {
            super(type, null);
        }

        @Override
        public Map<String, Object> getAttributes() {
            return null;
        }

        @Override
        public String attributesToString() {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    @Test
    public void testGetDistributionType() {
        Assert.assertEquals(new MockDistributionNode(Distribution.Type.QUANTILE).getDistributionType(), "QUANTILE");
        Assert.assertEquals(new MockDistributionNode(Distribution.Type.PMF).getDistributionType(), "FREQ");
        Assert.assertEquals(new MockDistributionNode(Distribution.Type.CDF).getDistributionType(), "CUMFREQ");
    }
}
