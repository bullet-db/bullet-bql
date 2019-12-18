/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.util.QueryUtil.simpleSortItem;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class OrderByNodeTest {
    private List<SortItemNode> sortItems;
    private OrderByNode orderBy;

    @BeforeClass
    public void setUp() {
        sortItems = singletonList(simpleSortItem());
        orderBy = new OrderByNode(sortItems);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QsortItems should not be empty\\E.*")
    public void testEmptySortItems() {
        new OrderByNode(emptyList());
    }

    @Test
    public void testGetChildren() {
        assertEquals(orderBy.getChildren(), sortItems);
    }

    @Test
    public void testEquals() {
        assertFalse(orderBy.equals(null));
        assertFalse(orderBy.equals(sortItems));
    }
}
