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

public class IsNotEmptyPredicateTest {
    private Expression value;
    private IsNotEmptyPredicate isNotEmptyPredicate;

    @BeforeClass
    public void setUp() {
        value = identifier("aaa");
        isNotEmptyPredicate = new IsNotEmptyPredicate(value);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = singletonList(value);
        assertEquals(isNotEmptyPredicate.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        IsNotEmptyPredicate copy = isNotEmptyPredicate;
        assertTrue(isNotEmptyPredicate.equals(copy));
        assertFalse(isNotEmptyPredicate.equals(null));
        assertFalse(isNotEmptyPredicate.equals(value));

        IsNotEmptyPredicate isNotEmptyPredicateDiffValue = new IsNotEmptyPredicate(identifier("bbb"));
        assertFalse(isNotEmptyPredicate.equals(isNotEmptyPredicateDiffValue));
    }

    @Test
    public void testHashCode() {
        IsNotEmptyPredicate sameIsNotEmptyPredicate = new IsNotEmptyPredicate(identifier("aaa"));
        assertEquals(isNotEmptyPredicate.hashCode(), sameIsNotEmptyPredicate.hashCode());
    }
}
