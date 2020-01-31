/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.parsing.expressions.Operation;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class BinaryExpressionNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new BinaryExpressionNode(identifier("abc"), identifier("def"), Operation.ADD),
                              new BinaryExpressionNode(identifier("def"), identifier("def"), Operation.ADD),
                              new BinaryExpressionNode(identifier("abc"), identifier("abc"), Operation.ADD),
                              new BinaryExpressionNode(identifier("abc"), identifier("def"), Operation.SUB));
    }
}
