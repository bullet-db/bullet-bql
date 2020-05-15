/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class SelectItemNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new SelectItemNode(false, new LiteralNode(5, null), identifier("abc"), null),
                                        new SelectItemNode(true, new LiteralNode(5, null), identifier("abc"), null),
                                        new SelectItemNode(false, new LiteralNode(true, null), identifier("abc"), null),
                                        new SelectItemNode(false, new LiteralNode(5, null), identifier("def"), null));
    }
}
