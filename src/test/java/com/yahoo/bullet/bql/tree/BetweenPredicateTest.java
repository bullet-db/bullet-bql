/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class BetweenPredicateTest {
    private Expression value;
    private Expression min;
    private Expression max;
    private BetweenPredicate betweenPredicate;

    @BeforeClass
    public void setUp() {
        value = identifier("aaa");
        min = new DoubleLiteral("5.5");
        max = new DoubleLiteral("10.0");
        betweenPredicate = new BetweenPredicate(value, min, max);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = ImmutableList.of(value, min, max);
        assertEquals(betweenPredicate.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        BetweenPredicate copy = betweenPredicate;
        assertTrue(betweenPredicate.equals(copy));
        assertFalse(betweenPredicate.equals(null));
        assertFalse(betweenPredicate.equals(min));

        BetweenPredicate betweenPredicateDiffValue = new BetweenPredicate(identifier("bbb"), min, max);
        assertFalse(betweenPredicate.equals(betweenPredicateDiffValue));

        BetweenPredicate betweenPredicateDiffMin = new BetweenPredicate(value, new DoubleLiteral("4.5"), max);
        assertFalse(betweenPredicate.equals(betweenPredicateDiffMin));

        BetweenPredicate betweenPredicateDiffMax = new BetweenPredicate(value, min, new DoubleLiteral("10.5"));
        assertFalse(betweenPredicate.equals(betweenPredicateDiffMax));
    }

    @Test
    public void testHashCode() {
        BetweenPredicate sameBetween = new BetweenPredicate(identifier("aaa"), min, max);
        assertEquals(betweenPredicate.hashCode(), sameBetween.hashCode());
    }
}
