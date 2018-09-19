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

import static com.yahoo.bullet.bql.tree.SortItem.NullOrdering.LAST;
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
    private Select select;
    private Stream stream;
    private Expression where;
    private SimpleGroupBy simpleGroupBy;
    private GroupBy groupBy;
    private Expression having;
    private OrderBy orderBy;
    private WindowInclude include;
    private Windowing windowing;

    @BeforeClass
    public void setUp() {
        select = selectList(identifier("aaa"));
        stream = new Stream(Optional.of("10"), Optional.of("20"));
        where = equal(identifier("bbb"), identifier("ccc"));
        simpleGroupBy = new SimpleGroupBy(singletonList(identifier("ddd")));
        groupBy = new GroupBy(true, singletonList(simpleGroupBy));
        having = equal(identifier("eee"), identifier("fff"));
        orderBy = simpleOrderBy();
        include = simpleWindowInclude();
        windowing = new Windowing((long) 100, TIME, include);
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
        String expected = "QuerySpecification{select=Select{distinct=false, selectItems=[aaa]}, " +
                "from=Optional[Stream{timeDuration=Optional[10], recordDuration=Optional[20]}], " +
                "where=(bbb = ccc), " +
                "groupBy=Optional[GroupBy{isDistinct=true, groupingElements=[SimpleGroupBy{columns=[ddd]}]}], " +
                "having=(eee = fff), orderBy=Optional[OrderBy{sortItems=[SortItem{sortKey=aaa, ordering=ASCENDING, nullOrdering=FIRST}]}], " +
                "limit=10, " +
                "windowing=Windowing{emitEvery=100, emitType=TIME, include=WindowInclude{type=Optional[LAST], unit=TIME, number=Optional[100]}}}";
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

        Select select2 = selectList(identifier("bbb"));
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

        Stream stream2 = new Stream(Optional.of("10"), Optional.of("30"));
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

        Expression where2 = equal(identifier("bbb"), identifier("aaa"));
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

        GroupBy groupBy2 = new GroupBy(false, singletonList(simpleGroupBy));
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

        Expression having2 = equal(identifier("eee"), identifier("ggg"));
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

        SortItem sortItem2 = new SortItem(identifier("aaa"), LAST);
        OrderBy orderBy2 = new OrderBy(singletonList(sortItem2));
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

        Windowing windowing2 = new Windowing((long) 200, TIME, include);
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
