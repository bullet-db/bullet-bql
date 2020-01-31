/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.parsing.expressions.Operation;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class NAryExpressionNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new NAryExpressionNode(Operation.AND, Collections.singletonList(identifier("abc"))),
                              new NAryExpressionNode(Operation.OR, Collections.singletonList(identifier("abc"))),
                              new NAryExpressionNode(Operation.AND, Collections.emptyList()));
    }
}
