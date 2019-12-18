/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class BooleanLiteralNodeTest {
    private BooleanLiteralNode booleanLiteral;

    @BeforeClass
    public void setUp() {
        booleanLiteral = new BooleanLiteralNode("TRUE");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidValue() {
        new BooleanLiteralNode("NOT VALID");
    }

    @Test
    public void testHashCode() {
        BooleanLiteralNode same = new BooleanLiteralNode("TRUE");
        assertEquals(booleanLiteral.hashCode(), same.hashCode());
    }

    @Test
    public void testEquals() {
        BooleanLiteralNode copy = booleanLiteral;
        assertTrue(booleanLiteral.equals(copy));
        assertFalse(booleanLiteral.equals(null));
        assertFalse(booleanLiteral.equals(identifier("aaa")));
    }
}
