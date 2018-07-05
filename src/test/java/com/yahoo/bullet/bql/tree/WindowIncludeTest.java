/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.yahoo.bullet.bql.tree.WindowInclude.IncludeType.FIRST;
import static com.yahoo.bullet.bql.tree.WindowInclude.IncludeType.LAST;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static com.yahoo.bullet.parsing.Window.Unit.RECORD;
import static com.yahoo.bullet.parsing.Window.Unit.TIME;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class WindowIncludeTest {
    private WindowInclude windowInclude;

    @BeforeClass
    public void setUp() {
        windowInclude = new WindowInclude(TIME, Optional.of(FIRST), Optional.of((long) 10));
    }

    @Test
    public void testGetChildren() {
        assertEquals(windowInclude.getChildren(), emptyList());
    }

    @Test
    public void testEquals() {
        WindowInclude copy = windowInclude;
        assertTrue(windowInclude.equals(copy));
        assertFalse(windowInclude.equals(null));
        assertFalse(windowInclude.equals(identifier("aaa")));

        WindowInclude windowIncludeDiffUnit = new WindowInclude(RECORD, Optional.of(FIRST), Optional.of((long) 10));
        assertFalse(windowInclude.equals(windowIncludeDiffUnit));

        WindowInclude windowIncludeDiffType = new WindowInclude(TIME, Optional.of(LAST), Optional.of((long) 10));
        assertFalse(windowInclude.equals(windowIncludeDiffType));

        WindowInclude windowIncludeDiffNumber = new WindowInclude(TIME, Optional.of(FIRST), Optional.of((long) 20));
        assertFalse(windowInclude.equals(windowIncludeDiffNumber));
    }
}
