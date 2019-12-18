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

public class DecimalLiteralNodeTest {
    private DecimalLiteralNode decimalLiteral;

    @BeforeClass
    public void setUp() {
        decimalLiteral = new DecimalLiteralNode("5.5");
    }

    @Test
    public void testEquals() {
        DecimalLiteralNode copy = decimalLiteral;
        assertTrue(decimalLiteral.equals(copy));
        assertFalse(decimalLiteral.equals(null));
        assertFalse(decimalLiteral.equals(identifier("aaa")));
    }

    @Test
    public void testHashCode() {
        DecimalLiteralNode same = new DecimalLiteralNode("5.5");
        assertEquals(decimalLiteral.hashCode(), decimalLiteral.hashCode());
    }
}
