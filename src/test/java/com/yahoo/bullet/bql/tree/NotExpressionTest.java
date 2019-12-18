/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.util.QueryUtil.equal;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class NotExpressionTest {
    private ExpressionNode value;
    private NotExpression notExpression;

    @BeforeClass
    public void setUp() {
        value = equal(identifier("aaa"), identifier("bbb"));
        notExpression = new NotExpression(value);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = singletonList(value);
        assertEquals(notExpression.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        NotExpression copy = notExpression;
        assertTrue(notExpression.equals(copy));
        assertFalse(notExpression.equals(null));
        assertFalse(notExpression.equals(value));
    }

    @Test
    public void testHashCode() {
        NotExpression same = new NotExpression(equal(identifier("aaa"), identifier("bbb")));
        assertEquals(notExpression.hashCode(), same.hashCode());
    }
}
