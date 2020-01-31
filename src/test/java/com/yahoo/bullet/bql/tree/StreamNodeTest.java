package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

public class StreamNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new StreamNode("2000"), new StreamNode("MAX"));
    }
}
