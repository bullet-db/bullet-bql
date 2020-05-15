/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.query.expressions.Operation;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class UnaryExpressionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new UnaryExpressionNode(Operation.SIZE_OF, identifier("abc"), false, null),
                                        new UnaryExpressionNode(Operation.IS_NULL, identifier("abc"), false, null),
                                        new UnaryExpressionNode(Operation.SIZE_OF, identifier("def"), false, null));
    }
}
