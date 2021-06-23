/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.Assert;
import org.testng.annotations.Test;

public class IdentifierNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new IdentifierNode("abc", true, null),
                                        new IdentifierNode("def", true, null));
    }

    @Test
    public void testEqualsAndHashCodeQuotedDoesntMatter() {
        IdentifierNode a = new IdentifierNode("abc", true, null);
        IdentifierNode b = new IdentifierNode("abc", false, null);
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }
}
