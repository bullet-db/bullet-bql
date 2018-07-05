/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.tree.ArithmeticUnaryExpression.Sign;
import static com.yahoo.bullet.bql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static com.yahoo.bullet.bql.tree.ArithmeticUnaryExpression.Sign.PLUS;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ArithmeticUnaryExpressionTest {
    private Expression value;
    private Sign sign;
    private NodeLocation location;
    private ArithmeticUnaryExpression arithmeticUnaryExpression;

    @BeforeClass
    public void setUp() {
        value = identifier("aaa");
        sign = PLUS;
        location = new NodeLocation(1, 1);
        arithmeticUnaryExpression = new ArithmeticUnaryExpression(location, sign, value);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = singletonList(value);
        assertEquals(arithmeticUnaryExpression.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        ArithmeticUnaryExpression copy = arithmeticUnaryExpression;
        assertTrue(arithmeticUnaryExpression.equals(copy));
        assertFalse(arithmeticUnaryExpression.equals(null));
        assertFalse(arithmeticUnaryExpression.equals(value));

        ArithmeticUnaryExpression arithmeticUnaryDiffValue = new ArithmeticUnaryExpression(location, sign, identifier("bbb"));
        assertFalse(arithmeticUnaryExpression.equals(arithmeticUnaryDiffValue));

        ArithmeticUnaryExpression arithmeticUnaryDiffSign = new ArithmeticUnaryExpression(location, MINUS, value);
        assertFalse(arithmeticUnaryExpression.equals(arithmeticUnaryDiffSign));
    }

    @Test
    public void testHashCode() {
        ArithmeticUnaryExpression sameArithmeticUnary = new ArithmeticUnaryExpression(location, sign, identifier("aaa"));
        assertEquals(arithmeticUnaryExpression.hashCode(), sameArithmeticUnary.hashCode());
    }
}
