/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.tree.SortItem.NullOrdering.FIRST;
import static com.yahoo.bullet.bql.tree.SortItem.NullOrdering.LAST;
import static com.yahoo.bullet.bql.tree.SortItem.Ordering.ASCENDING;
import static com.yahoo.bullet.bql.tree.SortItem.Ordering.DESCENDING;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SortItemTest {
    private SortItem sortItem;
    private Expression sortKey;

    @BeforeClass
    public void setUp() {
        sortKey = identifier("aaa");
        sortItem = new SortItem(sortKey, ASCENDING, FIRST);
    }

    @Test
    public void testGetSortKey() {
        assertEquals(sortItem.getSortKey(), sortKey);
    }

    @Test
    public void testGetOrdering() {
        assertEquals(sortItem.getOrdering(), ASCENDING);
    }

    @Test
    public void testGetNullOrdering() {
        assertEquals(sortItem.getNullOrdering(), FIRST);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = singletonList(sortKey);
        assertEquals(sortItem.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        assertFalse(sortItem.equals(null));
        assertFalse(sortItem.equals(sortKey));

        Expression diffSortKey = identifier("bbb");
        SortItem sortItemDiffSortKey = new SortItem(diffSortKey, ASCENDING, FIRST);
        assertFalse(sortItem.equals(sortItemDiffSortKey));

        SortItem sortItemDiffOrdering = new SortItem(sortKey, DESCENDING, FIRST);
        assertFalse(sortItem.equals(sortItemDiffOrdering));

        SortItem sortItemDiffNullOrdering = new SortItem(sortKey, ASCENDING, LAST);
        assertFalse(sortItem.equals(sortItemDiffNullOrdering));

        SortItem same = new SortItem(identifier("aaa"), ASCENDING, FIRST);
        assertTrue(sortItem.equals(same));
    }
}
