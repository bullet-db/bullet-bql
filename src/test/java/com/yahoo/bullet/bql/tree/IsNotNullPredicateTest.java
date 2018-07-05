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

public class IsNotNullPredicateTest {
    private Expression value;
    private IsNotNullPredicate isNotNullPredicate;

    @BeforeClass
    public void setUp() {
        value = identifier("aaa");
        isNotNullPredicate = new IsNotNullPredicate(value);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = singletonList(value);
        assertEquals(isNotNullPredicate.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        IsNotNullPredicate copy = isNotNullPredicate;
        assertTrue(isNotNullPredicate.equals(copy));
        assertFalse(isNotNullPredicate.equals(null));
        assertFalse(isNotNullPredicate.equals(value));
    }

    @Test
    public void testHashCode() {
        IsNotNullPredicate same = new IsNotNullPredicate(identifier("aaa"));
        assertEquals(isNotNullPredicate.hashCode(), same.hashCode());
    }
}
