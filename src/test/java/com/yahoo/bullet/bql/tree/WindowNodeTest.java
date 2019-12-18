/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.yahoo.bullet.bql.tree.WindowIncludeNode.IncludeType.FIRST;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleWindowInclude;
import static com.yahoo.bullet.parsing.Window.Unit.RECORD;
import static com.yahoo.bullet.parsing.Window.Unit.TIME;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class WindowNodeTest {
    private WindowIncludeNode windowInclude;
    private WindowNode windowing;

    @BeforeClass
    public void setUp() {
        windowInclude = simpleWindowInclude();
        windowing = new WindowNode((long) 10, TIME, windowInclude);
    }

    @Test
    public void testGetChildren() {
        assertEquals(windowing.getChildren(), emptyList());
    }

    @Test
    public void testEquals() {
        assertFalse(windowing.equals(null));
        assertFalse(windowing.equals(windowInclude));

        WindowIncludeNode diffWindowInclude = new WindowIncludeNode(TIME, Optional.of(FIRST), Optional.of((long) 100));
        WindowNode windowingDiffWindowInclude = new WindowNode((long) 10, TIME, diffWindowInclude);
        assertFalse(windowing.equals(windowingDiffWindowInclude));

        WindowNode windowingDiffType = new WindowNode((long) 10, RECORD, windowInclude);
        assertFalse(windowing.equals(windowingDiffType));
    }
}
