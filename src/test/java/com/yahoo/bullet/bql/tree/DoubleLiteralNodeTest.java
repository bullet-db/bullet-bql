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

public class DoubleLiteralNodeTest {
    private DoubleLiteralNode doubleLiteral;

    @BeforeClass
    public void setUp() {
        doubleLiteral = new DoubleLiteralNode("10.0");
    }

    @Test
    public void testEquals() {
        assertFalse(doubleLiteral.equals(null));
        assertFalse(doubleLiteral.equals(identifier("aaa")));
    }

    @Test
    public void testHashCode() {
        DoubleLiteralNode same = new DoubleLiteralNode("10.0");
        assertEquals(doubleLiteral.hashCode(), same.hashCode());

        DoubleLiteralNode doubleLiteralZero = new DoubleLiteralNode("0.0");
        DoubleLiteralNode doubleLiteralZeroSame = new DoubleLiteralNode("0.0");
        assertEquals(doubleLiteralZero.hashCode(), doubleLiteralZeroSame.hashCode());
    }
}
