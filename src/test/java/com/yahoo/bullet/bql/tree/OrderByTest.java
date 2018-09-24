/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleSortItem;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class OrderByTest {
    private List<SortItem> sortItems;
    private OrderBy.Ordering ordering;
    private OrderBy orderBy;

    @BeforeClass
    public void setUp() {
        sortItems = singletonList(simpleSortItem());
        ordering = OrderBy.Ordering.ASCENDING;
        orderBy = new OrderBy(sortItems, ordering);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QsortItems should not be empty\\E.*")
    public void testEmptySortItems() {
        new OrderBy(emptyList());
    }

    @Test
    public void testGetChildren() {
        assertEquals(orderBy.getChildren(), sortItems);
    }

    @Test
    public void testEquals() {
        OrderBy copy = orderBy;
        assertTrue(orderBy.equals(copy));
        assertFalse(orderBy.equals(null));
        assertFalse(orderBy.equals(sortItems));

        OrderBy orderByDiffSortItems = new OrderBy(singletonList(new SortItem(identifier("aaa"), SortItem.NullOrdering.UNDEFINED)), ordering);
        assertFalse(orderBy.equals(orderByDiffSortItems));

        OrderBy orderByDiffOrdering = new OrderBy(sortItems, OrderBy.Ordering.DESCENDING);
        assertFalse(orderBy.equals(orderByDiffOrdering));
    }
}
