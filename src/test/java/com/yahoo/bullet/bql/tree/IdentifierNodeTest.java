/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.util.QueryUtil.simpleInPredicate;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class IdentifierNodeTest {
    private IdentifierNode identifier;

    @BeforeClass
    public void setUp() {
        identifier = new IdentifierNode("aaa", true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\Qvalue contains illegal characters\\E.*")
    public void testInvalidName() {
        new IdentifierNode("!23aa", false);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = emptyList();
        assertEquals(identifier.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        assertFalse(identifier.equals(null));
        assertFalse(identifier.equals(simpleInPredicate()));
    }
}
