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

public class IsNullPredicateTest {
    private ExpressionNode value;
    private IsNullPredicate isNullPredicate;

    @BeforeClass
    public void setUp() {
        value = identifier("aaa");
        isNullPredicate = new IsNullPredicate(value);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = singletonList(value);
        assertEquals(isNullPredicate.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        IsNullPredicate copy = isNullPredicate;
        assertTrue(isNullPredicate.equals(copy));
        assertFalse(isNullPredicate.equals(null));
        assertFalse(isNullPredicate.equals(value));
    }

    @Test
    public void testHashCode() {
        IsNullPredicate same = new IsNullPredicate(identifier("aaa"));
        assertEquals(isNullPredicate.hashCode(), same.hashCode());
    }
}
