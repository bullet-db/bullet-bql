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

public class InfixExpressionTest {
    private Expression left;
    private Expression right;
    private String op;
    private InfixExpression infixExpression;

    @BeforeClass
    public void setUp() {
        left = identifier("aaa");
        right = new DoubleLiteral("5.0");
        op = "+";
        infixExpression = new InfixExpression(left, right, op);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = ImmutableList.of(left, right);
        assertEquals(infixExpression.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        InfixExpression copy = infixExpression;
        assertTrue(infixExpression.equals(copy));
        assertFalse(infixExpression.equals(null));
        assertFalse(infixExpression.equals(left));

        InfixExpression infixExpressionDiffLeft = new InfixExpression(identifier("bbb"), right, op);
        assertFalse(infixExpression.equals(infixExpressionDiffLeft));

        InfixExpression infixExpressionDiffRight = new InfixExpression(left, new DoubleLiteral("5.5"), op);
        assertFalse(infixExpression.equals(infixExpressionDiffRight));

        InfixExpression infixExpressionDiffOp = new InfixExpression(left, right, "-");
        assertFalse(infixExpression.equals(infixExpressionDiffOp));
    }

    @Test
    public void testHashCode() {
        InfixExpression sameInfixExpression = new InfixExpression(identifier("aaa"), new DoubleLiteral("5.0"), "+");
        assertEquals(infixExpression.hashCode(), sameInfixExpression.hashCode());
    }
}
