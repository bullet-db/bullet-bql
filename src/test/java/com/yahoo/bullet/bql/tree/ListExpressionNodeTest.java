/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class ListExpressionNodeTest extends ExpressionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new ListExpressionNode(Collections.singletonList(identifier("abc"))),
                              new ListExpressionNode(Collections.emptyList()));
    }
}
