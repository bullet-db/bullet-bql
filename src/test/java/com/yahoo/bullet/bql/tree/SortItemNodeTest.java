/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
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
