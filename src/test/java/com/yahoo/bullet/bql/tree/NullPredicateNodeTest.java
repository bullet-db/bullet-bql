/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class NullPredicateNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new NullPredicateNode(identifier("abc"), true, null),
                                        new NullPredicateNode(identifier("def"), true, null),
                                        new NullPredicateNode(identifier("abc"), false, null));
    }
}
