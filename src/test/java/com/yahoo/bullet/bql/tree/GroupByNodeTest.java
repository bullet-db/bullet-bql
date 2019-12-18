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
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class GroupByNodeTest {
    private List<GroupingElement> groupingElements;
    private GroupByNode groupBy;

    @BeforeClass
    public void setUp() {
        GroupingElement groupingElement = new SimpleGroupBy(singletonList(identifier("aaa")));
        groupingElements = singletonList(groupingElement);
        groupBy = new GroupByNode(false, groupingElements);
    }

    @Test
    public void testGetChildren() {
        assertEquals(groupBy.getChildren(), groupingElements);
    }

    @Test
    public void testEquals() {
        assertFalse(groupBy.equals(null));
        assertFalse(groupBy.equals(groupingElements));

        GroupingElement diffGroupingElement = new SimpleGroupBy(singletonList(identifier("bbb")));
        GroupByNode groupByDiffGroupingElements = new GroupByNode(false, singletonList(diffGroupingElement));
        assertFalse(groupBy.equals(groupByDiffGroupingElements));
    }
}
