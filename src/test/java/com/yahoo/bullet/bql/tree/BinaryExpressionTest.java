/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class BinaryExpressionTest {
    private Expression left;
    private Expression right;
    private String op;
    private BinaryExpression binaryExpression;

    @BeforeClass
    public void setUp() {
        left = identifier("aaa");
        right = new DoubleLiteral("5.0");
        op = "+";
        binaryExpression = new BinaryExpression(left, right, op);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = ImmutableList.of(left, right);
        assertEquals(binaryExpression.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        BinaryExpression copy = binaryExpression;
        assertTrue(binaryExpression.equals(copy));
        assertFalse(binaryExpression.equals(null));
        assertFalse(binaryExpression.equals(left));

        BinaryExpression binaryExpressionDiffLeft = new BinaryExpression(identifier("bbb"), right, op);
        assertFalse(binaryExpression.equals(binaryExpressionDiffLeft));

        BinaryExpression binaryExpressionDiffRight = new BinaryExpression(left, new DoubleLiteral("5.5"), op);
        assertFalse(binaryExpression.equals(binaryExpressionDiffRight));

        BinaryExpression binaryExpressionDiffOp = new BinaryExpression(left, right, "-");
        assertFalse(binaryExpression.equals(binaryExpressionDiffOp));
    }

    @Test
    public void testHashCode() {
        BinaryExpression sameBinaryExpression = new BinaryExpression(identifier("aaa"), new DoubleLiteral("5.0"), "+");
        assertEquals(binaryExpression.hashCode(), sameBinaryExpression.hashCode());
    }
}
