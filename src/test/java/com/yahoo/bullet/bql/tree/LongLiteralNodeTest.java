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

public class LongLiteralNodeTest {
    private LongLiteralNode longLiteral;

    @BeforeClass
    public void setUp() {
        longLiteral = new LongLiteralNode("100");
    }

    @Test
    public void testEquals() {
        LongLiteralNode copy = longLiteral;
        assertTrue(longLiteral.equals(copy));
        assertFalse(longLiteral.equals(null));
        assertFalse(longLiteral.equals(identifier("aaa")));

        LongLiteralNode longLiteralDiffValue = new LongLiteralNode("200");
        assertFalse(longLiteral.equals(longLiteralDiffValue));
    }

    @Test
    public void testHashCode() {
        LongLiteralNode same = new LongLiteralNode("100");
        assertEquals(longLiteral.hashCode(), same.hashCode());
    }
}
