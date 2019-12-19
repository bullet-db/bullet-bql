/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class SelectNodeTest {
    private List<SelectItemNode> selectItems;
    private SelectNode select;

    /*@BeforeClass
    public void setUp() {
        selectItems = singletonList(new AllColumns());
        select = new SelectNode(true, selectItems);
    }

    @Test
    public void testGetChildren() {
        assertEquals(select.getChildren(), selectItems);
    }

    @Test
    public void testEquals() {
        assertFalse(select.equals(null));
        assertFalse(select.equals(selectItems));

        SelectNode selectDiffDistinct = new SelectNode(false, selectItems);
        assertFalse(select.equals(selectDiffDistinct));
    }*/
}
