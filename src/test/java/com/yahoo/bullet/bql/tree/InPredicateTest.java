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
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class InPredicateTest {
    private InPredicate inPredicate;
    private ExpressionNode value;
    private ExpressionNode valueList;

    @BeforeClass
    public void setUp() {
        value = identifier("aaa");
        valueList = new ListExpressionNode(singletonList(identifier("bbb")));
        inPredicate = new InPredicate(value, valueList);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = ImmutableList.of(value, valueList);
        assertEquals(inPredicate.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        InPredicate copy = inPredicate;
        assertTrue(inPredicate.equals(copy));
        assertFalse(inPredicate.equals(null));
        assertFalse(inPredicate.equals(value));

        ExpressionNode diffValue = identifier("ccc");
        InPredicate inPredicateDiffValue = new InPredicate(diffValue, valueList);
        assertFalse(inPredicate.equals(inPredicateDiffValue));

        ExpressionNode diffValueList = new ListExpressionNode(singletonList(identifier("ddd")));
        InPredicate inPredicateDiffValueList = new InPredicate(value, diffValueList);
        assertFalse(inPredicate.equals(inPredicateDiffValueList));
    }

    @Test
    public void testHashCode() {
        InPredicate same = new InPredicate(identifier("aaa"), valueList);
        assertEquals(inPredicate.hashCode(), same.hashCode());
    }
}
