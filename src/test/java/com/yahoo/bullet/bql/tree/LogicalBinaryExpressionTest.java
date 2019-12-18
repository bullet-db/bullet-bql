/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.tree.LogicalBinaryExpression.and;
import static com.yahoo.bullet.bql.tree.LogicalBinaryExpression.or;
import static com.yahoo.bullet.bql.util.QueryUtil.equal;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class LogicalBinaryExpressionTest {
    private ExpressionNode left;
    private ExpressionNode right;
    private LogicalBinaryExpression and;

    @BeforeClass
    public void setUp() {
        left = equal(identifier("aaa"), identifier("bbb"));
        right = equal(identifier("ccc"), identifier("ddd"));
        and = and(left, right);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = asList(left, right);
        assertEquals(and.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        LogicalBinaryExpression copy = and;
        assertTrue(and.equals(copy));
        assertFalse(and.equals(null));
        assertFalse(and.equals(identifier("aaa")));

        LogicalBinaryExpression or = or(left, right);
        assertFalse(and.equals(or));

        ExpressionNode diffHalf = equal(identifier("aaa"), identifier("ccc"));
        LogicalBinaryExpression andDiffLeft = and(diffHalf, right);
        assertFalse(and.equals(andDiffLeft));

        LogicalBinaryExpression andDiffRight = and(left, diffHalf);
        assertFalse(and.equals(andDiffRight));
    }

    @Test
    public void testHashCode() {
        ExpressionNode sameLeft = equal(identifier("aaa"), identifier("bbb"));
        LogicalBinaryExpression sameAnd = and(sameLeft, right);
        assertEquals(and.hashCode(), sameAnd.hashCode());
    }
}
