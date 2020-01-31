package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

public class SortItemNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new SortItemNode(new LiteralNode(5), SortItemNode.Ordering.ASCENDING),
                              new SortItemNode(new LiteralNode(true), SortItemNode.Ordering.ASCENDING),
                              new SortItemNode(new LiteralNode(5), SortItemNode.Ordering.DESCENDING));
    }
}
