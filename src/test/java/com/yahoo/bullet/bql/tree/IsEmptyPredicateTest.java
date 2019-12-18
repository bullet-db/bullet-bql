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

public class IsEmptyPredicateTest {
    private ExpressionNode value;
    private IsEmptyPredicate isEmptyPredicate;

    @BeforeClass
    public void setUp() {
        value = identifier("aaa");
        isEmptyPredicate = new IsEmptyPredicate(value);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = singletonList(value);
        assertEquals(isEmptyPredicate.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        IsEmptyPredicate copy = isEmptyPredicate;
        assertTrue(isEmptyPredicate.equals(copy));
        assertFalse(isEmptyPredicate.equals(null));
        assertFalse(isEmptyPredicate.equals(value));

        IsEmptyPredicate isEmptyPredicateDiffValue = new IsEmptyPredicate(identifier("bbb"));
        assertFalse(isEmptyPredicate.equals(isEmptyPredicateDiffValue));
    }

    @Test
    public void testHashCode() {
        IsEmptyPredicate sameIsEmptyPredicate = new IsEmptyPredicate(identifier("aaa"));
        assertEquals(isEmptyPredicate.hashCode(), sameIsEmptyPredicate.hashCode());
    }
}
