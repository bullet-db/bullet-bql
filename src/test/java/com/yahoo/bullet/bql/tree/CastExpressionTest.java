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

public class CastExpressionTest {
    private Expression expression;
    private String castType;
    private CastExpression castExpression;

    @BeforeClass
    public void setUp() {
        expression = identifier("aaa");
        castType = "FLOAT";
        castExpression = new CastExpression(expression, castType);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = ImmutableList.of(expression);
        assertEquals(castExpression.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        CastExpression copy = castExpression;
        assertTrue(castExpression.equals(copy));
        assertFalse(castExpression.equals(null));
        assertFalse(castExpression.equals(expression));

        CastExpression castExpressionDiffExpression = new CastExpression(identifier("bbb"), castType);
        assertFalse(castExpression.equals(castExpressionDiffExpression));

        CastExpression castExpressionDiffCastType = new CastExpression(expression, "DOUBLE");
        assertFalse(castExpression.equals(castExpressionDiffCastType));
    }

    @Test
    public void testHashCode() {
        CastExpression sameCastExpression = new CastExpression(identifier("aaa"), "FLOAT");
        assertEquals(castExpression.hashCode(), sameCastExpression.hashCode());
    }
}
