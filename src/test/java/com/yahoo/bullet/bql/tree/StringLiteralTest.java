/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class StringLiteralTest {
    private StringLiteral stringLiteral;

    @BeforeClass
    public void setUp() {
        stringLiteral = new StringLiteral("aaa");
    }

    @Test
    public void testEquals() {
        assertFalse(stringLiteral.equals(null));
        assertFalse(stringLiteral.equals("aaa"));
    }

    @Test
    public void testHashCode() {
        StringLiteral same = new StringLiteral("aaa");
        assertEquals(stringLiteral.hashCode(), same.hashCode());
    }
}
