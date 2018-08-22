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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ReferenceWithFunctionTest {
    private ReferenceWithFunction referenceWithFunction;
    private Expression value;
    private Operation op;

    @BeforeClass
    public void setUp() {
        value = identifier("aaa");
        op = Operation.SIZE_OF;
        referenceWithFunction = new ReferenceWithFunction(op, value);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = ImmutableList.of(value);
        assertEquals(referenceWithFunction.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        ReferenceWithFunction copy = referenceWithFunction;
        assertTrue(referenceWithFunction.equals(copy));
        assertFalse(referenceWithFunction.equals(null));
        assertFalse(referenceWithFunction.equals(value));

        Expression diffValue = identifier("ccc");
        ReferenceWithFunction referenceWithFunctionDiffValue = new ReferenceWithFunction(op, diffValue);
        assertFalse(referenceWithFunction.equals(referenceWithFunctionDiffValue));
    }

    @Test
    public void testHashCode() {
        ReferenceWithFunction same = new ReferenceWithFunction(op, identifier("aaa"));
        assertEquals(referenceWithFunction.hashCode(), same.hashCode());
    }
}
