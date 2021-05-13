/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class BetweenPredicateNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new BetweenPredicateNode(identifier("abc"), identifier("def"), identifier("ghi"), true, null),
                                        new BetweenPredicateNode(identifier("---"), identifier("def"), identifier("ghi"), true, null),
                                        new BetweenPredicateNode(identifier("abc"), identifier("---"), identifier("ghi"), true, null),
                                        new BetweenPredicateNode(identifier("abc"), identifier("def"), identifier("---"), true, null),
                                        new BetweenPredicateNode(identifier("abc"), identifier("def"), identifier("ghi"), false, null));
    }
}
