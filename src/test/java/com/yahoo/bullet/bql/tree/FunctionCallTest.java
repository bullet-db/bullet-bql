/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.MAX;
import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType.MIN;
import static com.yahoo.bullet.bql.tree.SortItemNode.NullOrdering.FIRST;
import static com.yahoo.bullet.bql.tree.SortItemNode.Ordering.ASCENDING;
import static com.yahoo.bullet.bql.util.QueryUtil.equal;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleOrderBy;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class FunctionCallTest {
    private GroupOperationType type;
    private ExpressionNode filter;
    private OrderByNode orderBy;
    private boolean distinct;
    private ExpressionNode argument;
    private FunctionCall functionCall;

    @BeforeClass
    public void setUp() {
        type = MIN;
        filter = equal(identifier("aaa"), identifier("bbb"));
        orderBy = simpleOrderBy();
        distinct = false;
        argument = identifier("ccc");
        functionCall = new FunctionCall(type, Optional.of(filter), Optional.of(orderBy), distinct, singletonList(argument));
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = ImmutableList.of(filter, orderBy.getSortItems().get(0), argument);
        assertEquals(functionCall.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        FunctionCall copy = functionCall;
        assertTrue(functionCall.equals(copy));
        assertFalse(functionCall.equals(null));
        assertFalse(functionCall.equals(type));

        FunctionCall functionCallDiffType = new FunctionCall(MAX, Optional.of(filter), Optional.of(orderBy), distinct, singletonList(argument));
        assertFalse(functionCall.equals(functionCallDiffType));

        ExpressionNode diffFilter = equal(identifier("ccc"), identifier("bbb"));
        FunctionCall functionCallDiffFilter = new FunctionCall(type, Optional.of(diffFilter), Optional.of(orderBy), distinct, singletonList(argument));
        assertFalse(functionCall.equals(functionCallDiffFilter));

        SortItemNode diffSortItem = new SortItemNode(identifier("ccc"), ASCENDING, FIRST);
        OrderByNode diffOrderBy = new OrderByNode(singletonList(diffSortItem));
        FunctionCall functionCallDiffOrderBy = new FunctionCall(type, Optional.of(filter), Optional.of(diffOrderBy), distinct, singletonList(argument));
        assertFalse(functionCall.equals(functionCallDiffOrderBy));

        ExpressionNode diffArgument = identifier("fff");
        FunctionCall functionCallDiffArg = new FunctionCall(type, Optional.of(filter), Optional.of(orderBy), distinct, singletonList(diffArgument));
        assertFalse(functionCall.equals(functionCallDiffArg));

        FunctionCall functionCallDiffDistinct = new FunctionCall(type, Optional.of(filter), Optional.of(orderBy), !distinct, singletonList(argument));
        assertFalse(functionCall.equals(functionCallDiffDistinct));

    }
}
