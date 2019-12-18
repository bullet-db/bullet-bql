/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import com.yahoo.bullet.parsing.Clause.Operation;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ContainsPredicateTest {
    private ContainsPredicate containsPredicate;
    private ExpressionNode value;
    private ExpressionNode valueList;
    private Operation op;

    @BeforeClass
    public void setUp() {
        value = identifier("aaa");
        valueList = new ListExpressionNode(singletonList(identifier("bbb")));
        op = Operation.EQUALS;
        containsPredicate = new ContainsPredicate(op, value, valueList);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = ImmutableList.of(value, valueList);
        assertEquals(containsPredicate.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        ContainsPredicate copy = containsPredicate;
        assertTrue(containsPredicate.equals(copy));
        assertFalse(containsPredicate.equals(null));
        assertFalse(containsPredicate.equals(value));

        ExpressionNode diffValue = identifier("ccc");
        ContainsPredicate conatainsPredicateDiffValue = new ContainsPredicate(op, diffValue, valueList);
        assertFalse(containsPredicate.equals(conatainsPredicateDiffValue));

        ExpressionNode diffValueList = new ListExpressionNode(singletonList(identifier("ddd")));
        ContainsPredicate containsPredicateDiffValueList = new ContainsPredicate(op, value, diffValueList);
        assertFalse(containsPredicate.equals(containsPredicateDiffValueList));
    }

    @Test
    public void testHashCode() {
        ContainsPredicate same = new ContainsPredicate(op, identifier("aaa"), valueList);
        assertEquals(containsPredicate.hashCode(), same.hashCode());
    }
}
