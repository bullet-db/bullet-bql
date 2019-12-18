/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.bql.tree.SortItemNode.NullOrdering.LAST;
import static com.yahoo.bullet.bql.tree.SortItemNode.Ordering.ASCENDING;
import static com.yahoo.bullet.bql.util.QueryUtil.equal;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static com.yahoo.bullet.bql.util.QueryUtil.selectList;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleOrderBy;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleWindowInclude;
import static com.yahoo.bullet.parsing.Window.Unit.TIME;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class QuerySpecificationTest {
    private QuerySpecification querySpecification;
    private SelectNode select;
    private StreamNode stream;
    private ExpressionNode where;
    private SimpleGroupBy simpleGroupBy;
    private GroupByNode groupBy;
    private ExpressionNode having;
    private OrderByNode orderBy;
    private WindowIncludeNode include;
    private WindowNode windowing;

    @BeforeClass
    public void setUp() {
        select = selectList(identifier("aaa"));
        stream = new StreamNode(Optional.of("10"), Optional.of("20"));
        where = equal(identifier("bbb"), identifier("ccc"));
        simpleGroupBy = new SimpleGroupBy(singletonList(identifier("ddd")));
        groupBy = new GroupByNode(true, singletonList(simpleGroupBy));
        having = equal(identifier("eee"), identifier("fff"));
        orderBy = simpleOrderBy();
        include = simpleWindowInclude();
        windowing = new WindowNode((long) 100, TIME, include);
        querySpecification = new QuerySpecification(
                select,
                Optional.of(stream),
                Optional.of(where),
                Optional.of(groupBy),
                Optional.of(having),
                Optional.of(orderBy),
                Optional.of("10"),
                Optional.of(windowing));
    }

    @Test
    public void testToString() {
        String expected = "QuerySpecification{select=SelectNode{distinct=false, selectItems=[aaa]}, " +
                "from=Optional[StreamNode{timeDuration=Optional[10], recordDuration=Optional[20]}], " +
                "where=(bbb = ccc), " +
                "groupBy=Optional[GroupByNode{isDistinct=true, groupingElements=[SimpleGroupBy{columns=[ddd]}]}], " +
                "having=(eee = fff), orderBy=Optional[OrderByNode{sortItems=[SortItemNode{sortKey=aaa, ordering=ASCENDING, nullOrdering=FIRST}]}], " +
                "limit=10, " +
                "windowing=WindowNode{emitEvery=100, emitType=TIME, include=WindowIncludeNode{type=Optional[LAST], unit=TIME, number=Optional[100]}}}";
        assertEquals(querySpecification.toString(), expected);
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = asList(select, stream, where, groupBy, having, orderBy, windowing);
        assertEquals(querySpecification.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        QuerySpecification that = querySpecification;
        assertTrue(querySpecification.equals(that));
        assertFalse(querySpecification.equals(null));
        assertFalse(querySpecification.equals(select));

        SelectNode select2 = selectList(identifier("bbb"));
        QuerySpecification querySpecificationDiffSelect = new QuerySpecification(
                select2,
                Optional.of(stream),
                Optional.of(where),
                Optional.of(groupBy),
                Optional.of(having),
                Optional.of(orderBy),
                Optional.of("10"),
                Optional.of(windowing));
        assertFalse(querySpecification.equals(querySpecificationDiffSelect));

        StreamNode stream2 = new StreamNode(Optional.of("10"), Optional.of("30"));
        QuerySpecification querySpecificationDiffStream = new QuerySpecification(
                select,
                Optional.of(stream2),
                Optional.of(where),
                Optional.of(groupBy),
                Optional.of(having),
                Optional.of(orderBy),
                Optional.of("10"),
                Optional.of(windowing));
        assertFalse(querySpecification.equals(querySpecificationDiffStream));

        ExpressionNode where2 = equal(identifier("bbb"), identifier("aaa"));
        QuerySpecification querySpecificationDiffWhere = new QuerySpecification(
                select,
                Optional.of(stream),
                Optional.of(where2),
                Optional.of(groupBy),
                Optional.of(having),
                Optional.of(orderBy),
                Optional.of("10"),
                Optional.of(windowing));
        assertFalse(querySpecification.equals(querySpecificationDiffWhere));

        GroupByNode groupBy2 = new GroupByNode(false, singletonList(simpleGroupBy));
        QuerySpecification querySpecificationDiffGroupBy = new QuerySpecification(
                select,
                Optional.of(stream),
                Optional.of(where),
                Optional.of(groupBy2),
                Optional.of(having),
                Optional.of(orderBy),
                Optional.of("10"),
                Optional.of(windowing));
        assertFalse(querySpecification.equals(querySpecificationDiffGroupBy));

        ExpressionNode having2 = equal(identifier("eee"), identifier("ggg"));
        QuerySpecification querySpecificationDiffHaving = new QuerySpecification(
                select,
                Optional.of(stream),
                Optional.of(where),
                Optional.of(groupBy),
                Optional.of(having2),
                Optional.of(orderBy),
                Optional.of("10"),
                Optional.of(windowing));
        assertFalse(querySpecification.equals(querySpecificationDiffHaving));

        SortItemNode sortItem2 = new SortItemNode(identifier("aaa"), ASCENDING, LAST);
        OrderByNode orderBy2 = new OrderByNode(singletonList(sortItem2));
        QuerySpecification querySpecificationDiffOrderBy = new QuerySpecification(
                select,
                Optional.of(stream),
                Optional.of(where),
                Optional.of(groupBy),
                Optional.of(having),
                Optional.of(orderBy2),
                Optional.of("10"),
                Optional.of(windowing));
        assertFalse(querySpecification.equals(querySpecificationDiffOrderBy));

        QuerySpecification querySpecificationDiffLimit = new QuerySpecification(
                select,
                Optional.of(stream),
                Optional.of(where),
                Optional.of(groupBy),
                Optional.of(having),
                Optional.of(orderBy),
                Optional.of("20"),
                Optional.of(windowing));
        assertFalse(querySpecification.equals(querySpecificationDiffLimit));

        WindowNode windowing2 = new WindowNode((long) 200, TIME, include);
        QuerySpecification querySpecificationDiffWindowing = new QuerySpecification(
                select,
                Optional.of(stream),
                Optional.of(where),
                Optional.of(groupBy),
                Optional.of(having),
                Optional.of(orderBy),
                Optional.of("10"),
                Optional.of(windowing2));
        assertFalse(querySpecification.equals(querySpecificationDiffWindowing));
    }

    @Test
    public void testHashCode() {
        QuerySpecification another = new QuerySpecification(
                select,
                Optional.of(stream),
                Optional.of(where),
                Optional.of(groupBy),
                Optional.of(having),
                Optional.of(orderBy),
                Optional.of("10"),
                Optional.of(windowing));
        assertEquals(querySpecification.hashCode(), another.hashCode());
    }
}
