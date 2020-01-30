/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import java.util.Collections;

public class CountDistinctNodeTest extends ExpressionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new CountDistinctNode(Collections.singletonList(new LiteralNode(5))),
                              new CountDistinctNode(Collections.emptyList()));
    }
}
