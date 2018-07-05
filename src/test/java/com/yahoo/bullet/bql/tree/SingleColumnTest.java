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
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SingleColumnTest {
    private Identifier alias;
    private Identifier expression;
    private SingleColumn singleColumn;

    @BeforeClass
    public void setUp() {
        alias = identifier("aaa");
        expression = identifier("bbb");
        singleColumn = new SingleColumn(expression, alias);
    }

    @Test
    public void testEquals() {
        SingleColumn copy = singleColumn;
        assertTrue(singleColumn.equals(copy));
        assertFalse(singleColumn.equals(null));
        assertFalse(singleColumn.equals(alias));

        Identifier diff = identifier("ccc");
        SingleColumn singleColumnDiffAlias = new SingleColumn(expression, diff);
        assertFalse(singleColumn.equals(singleColumnDiffAlias));

        SingleColumn singleColumnDiffExpression = new SingleColumn(diff, alias);
        assertFalse(singleColumn.equals(singleColumnDiffExpression));
    }

    @Test
    public void testToString() {
        String actual = singleColumn.toString();
        String expected = "bbb aaa";
        assertEquals(actual, expected);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = singletonList(expression);
        assertEquals(singleColumn.getChildren(), expected);
    }
}
