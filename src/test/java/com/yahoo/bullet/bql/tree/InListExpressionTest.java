/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class InListExpressionTest {
    private List<Expression> values;
    private InListExpression inListExpression;

    @Test
    public void setUp() {
        values = singletonList(identifier("aaa"));
        inListExpression = new InListExpression(values);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\Qvalues cannot be empty\\E.*")
    public void testEmptyValues() {
        new InListExpression(emptyList());
    }

    @Test
    public void testGetChildren() {
        assertEquals(inListExpression.getChildren(), values);
    }

    @Test
    public void testEquals() {
        assertFalse(inListExpression.equals(null));
        assertFalse(inListExpression.equals(values));
    }
}
