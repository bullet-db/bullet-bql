/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

public class SortItemNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new SortItemNode(new LiteralNode(5, null), SortItemNode.Ordering.ASCENDING, null),
                                        new SortItemNode(new LiteralNode(true, null), SortItemNode.Ordering.ASCENDING, null),
                                        new SortItemNode(new LiteralNode(5, null), SortItemNode.Ordering.DESCENDING, null));
    }
}
