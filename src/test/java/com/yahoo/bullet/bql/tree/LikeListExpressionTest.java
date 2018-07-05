/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class LikeListExpressionTest {
    private List<Expression> values;
    private LikeListExpression likeListExpression;

    @BeforeClass
    public void setUp() {
        values = singletonList(identifier("aaa"));
        likeListExpression = new LikeListExpression(values);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\Qvalues cannot be empty\\E.*")
    public void testEmptyValues() {
        new LikeListExpression(emptyList());
    }

    @Test
    public void testGetChildren() {
        assertEquals(likeListExpression.getChildren(), values);
    }

    @Test
    public void testEquals() {
        LikeListExpression copy = likeListExpression;
        assertTrue(likeListExpression.equals(copy));
        assertFalse(likeListExpression.equals(null));
        assertFalse(likeListExpression.equals(values));
    }
}
