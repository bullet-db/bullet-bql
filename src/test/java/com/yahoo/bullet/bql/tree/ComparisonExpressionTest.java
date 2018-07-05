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
import static com.yahoo.bullet.parsing.Clause.Operation.EQUALS;
import static com.yahoo.bullet.parsing.Clause.Operation.GREATER_EQUALS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class ComparisonExpressionTest {
    Expression left;
    Expression right;
    private ComparisonExpression comparisonExpression;

    @BeforeClass
    public void setUp() {
        left = identifier("aaa");
        right = identifier("bbb");
        comparisonExpression = new ComparisonExpression(EQUALS, left, right, false);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = ImmutableList.of(left, right);
        assertEquals(comparisonExpression.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        assertFalse(comparisonExpression.equals(null));
        assertFalse(comparisonExpression.equals(left));

        ComparisonExpression comparisonExpressionDiffOperation = new ComparisonExpression(GREATER_EQUALS, left, right, false);
        assertFalse(comparisonExpression.equals(comparisonExpressionDiffOperation));
    }
}
