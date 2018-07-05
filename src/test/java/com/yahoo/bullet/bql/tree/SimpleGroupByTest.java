/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SimpleGroupByTest {
    private List<Expression> columns;
    private SimpleGroupBy simpleGroupBy;

    @BeforeClass
    public void setUp() {
        columns = singletonList(identifier("aaa"));
        simpleGroupBy = new SimpleGroupBy(columns);
    }

    @Test
    public void testEnumerateGroupingSets() {
        List<Set<Expression>> expected = ImmutableList.of(ImmutableSet.copyOf(columns));
        assertEquals(simpleGroupBy.enumerateGroupingSets(), expected);
    }

    @Test
    public void testGetChildren() {
        assertEquals(simpleGroupBy.getChildren(), columns);
    }

    @Test
    public void testEquals() {
        SimpleGroupBy copy = simpleGroupBy;
        assertTrue(simpleGroupBy.equals(copy));
        assertFalse(simpleGroupBy.equals(null));
        assertFalse(simpleGroupBy.equals(columns));
    }
}
