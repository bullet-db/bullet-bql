/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.Assert;

import java.util.function.Supplier;

public class NodeUtils {
    /**
     * Helper for testing equals() and hashCode() in classes that extend {@link Node}.
     *
     * @param supplier A supplier that constructs the node to compare to.
     * @param nodes The other nodes to compare to that should be not equal.
     */
    public static void testEqualsAndHashCode(Supplier<Node> supplier, Node... nodes) {
        Node node = supplier.get();
        Assert.assertEquals(node, node);
        Assert.assertEquals(node.hashCode(), node.hashCode());

        for (Node other : nodes) {
            Assert.assertNotEquals(node, other);
            Assert.assertNotEquals(node.hashCode(), other.hashCode());
        }

        Node other = supplier.get();
        Assert.assertEquals(node, other);
        Assert.assertEquals(node.hashCode(), other.hashCode());

        // coverage
        Assert.assertFalse(node.equals(null));
    }
}
