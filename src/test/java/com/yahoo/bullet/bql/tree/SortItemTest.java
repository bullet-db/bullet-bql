/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.yahoo.bullet.bql.tree.SortItemNode.Ordering.ASCENDING;
import static com.yahoo.bullet.bql.tree.SortItemNode.Ordering.DESCENDING;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SortItemTest {
    private SortItemNode sortItem;
    private ExpressionNode sortKey;

    @BeforeClass
    public void setUp() {
        sortKey = identifier("aaa");
        sortItem = new SortItemNode(sortKey, ASCENDING);
    }

    /*@Test
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

        ExpressionNode diffSortKey = identifier("bbb");
        SortItemNode sortItemDiffSortKey = new SortItemNode(diffSortKey, ASCENDING, FIRST);
        assertFalse(sortItem.equals(sortItemDiffSortKey));

        SortItemNode sortItemDiffOrdering = new SortItemNode(sortKey, DESCENDING, FIRST);
        assertFalse(sortItem.equals(sortItemDiffOrdering));

        SortItemNode sortItemDiffNullOrdering = new SortItemNode(sortKey, ASCENDING, LAST);
        assertFalse(sortItem.equals(sortItemDiffNullOrdering));

        SortItemNode same = new SortItemNode(identifier("aaa"), ASCENDING, FIRST);
        assertTrue(sortItem.equals(same));
    }*/
}
