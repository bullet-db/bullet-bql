package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import java.util.Collections;

public class OrderByNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new OrderByNode(Collections.emptyList()), new OrderByNode(null));
    }
}
