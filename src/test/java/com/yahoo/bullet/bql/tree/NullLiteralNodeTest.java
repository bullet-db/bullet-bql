/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class NullLiteralNodeTest {
    private NullLiteralNode nullLiteral;

    @BeforeClass
    public void setUp() {
        nullLiteral = new NullLiteralNode();
    }

    @Test
    public void testEquals() {
        NullLiteralNode copy = nullLiteral;
        assertTrue(nullLiteral.equals(copy));
        assertFalse(nullLiteral.equals(null));
        assertFalse(nullLiteral.equals(identifier("aaa")));
    }

    @Test
    public void testGetChildren() {
        assertEquals(nullLiteral.getChildren(), emptyList());
    }

    @Test
    public void testHashCode() {
        NullLiteralNode same = new NullLiteralNode();
        assertEquals(nullLiteral.hashCode(), same.hashCode());
    }
}
