/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

public class WindowIncludeNodeTest {
    private WindowIncludeNode windowInclude;

    /*@BeforeClass
    public void setUp() {
        windowInclude = new WindowIncludeNode(TIME, Optional.of(FIRST), Optional.of((long) 10));
    }

    @Test
    public void testGetChildren() {
        assertEquals(windowInclude.getChildren(), emptyList());
    }

    @Test
    public void testEquals() {
        WindowIncludeNode copy = windowInclude;
        assertTrue(windowInclude.equals(copy));
        assertFalse(windowInclude.equals(null));
        assertFalse(windowInclude.equals(identifier("aaa")));

        WindowIncludeNode windowIncludeDiffUnit = new WindowIncludeNode(RECORD, Optional.of(FIRST), Optional.of((long) 10));
        assertFalse(windowInclude.equals(windowIncludeDiffUnit));

        WindowIncludeNode windowIncludeDiffType = new WindowIncludeNode(TIME, Optional.of(LAST), Optional.of((long) 10));
        assertFalse(windowInclude.equals(windowIncludeDiffType));

        WindowIncludeNode windowIncludeDiffNumber = new WindowIncludeNode(TIME, Optional.of(FIRST), Optional.of((long) 20));
        assertFalse(windowInclude.equals(windowIncludeDiffNumber));
    }*/
}
