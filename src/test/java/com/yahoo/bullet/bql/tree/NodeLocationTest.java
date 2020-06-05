package com.yahoo.bullet.bql.tree;

import org.testng.Assert;
import org.testng.annotations.Test;

public class NodeLocationTest {
    @Test
    public void testToString() {
        Assert.assertEquals(new NodeLocation(1, 5).toString(), "1:6: ");
        Assert.assertEquals(new NodeLocation(2, 0).toString(), "2:1: ");
        Assert.assertEquals(new NodeLocation(10, 10).toString(), "10:11: ");
    }
}
