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

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static com.yahoo.bullet.bql.util.QueryUtil.selectList;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleQuery;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class WithQueryTest {
    private WithQuery withQuery;
    private IdentifierNode name;
    private Query query;
    private List<IdentifierNode> columnNames;

    @BeforeClass
    public void setUp() {
        name = identifier("aaa");
        query = simpleQuery(selectList(identifier("bbb")));
        columnNames = singletonList(identifier("ccc"));
        NodeLocation location = new NodeLocation(1, 1);
        withQuery = new WithQuery(location, name, query, Optional.of(columnNames));
    }

    @Test
    public void testGetName() {
        assertEquals(withQuery.getName(), name);
    }

    @Test
    public void testGetQuery() {
        assertEquals(withQuery.getQuery(), query);
    }

    @Test
    public void testGetColumnNames() {
        assertEquals(withQuery.getColumnNames(), Optional.of(columnNames));
    }

    @Test
    public void testGetChildren() {
        assertEquals(withQuery.getChildren(), singletonList(query));
    }

    @Test
    public void testToString() {
        String actual = withQuery.toString();
        String expected = "WithQuery{" +
                "name=aaa, " +
                "query=Query{" +
                "queryBody=QuerySpecification{" +
                "select=SelectNode{distinct=false, selectItems=[bbb]}, " +
                "from=Optional.empty, " +
                "where=null, " +
                "groupBy=Optional.empty, " +
                "having=null, " +
                "orderBy=Optional.empty, " +
                "limit=null, " +
                "windowing=null}, " +
                "orderBy=Optional.empty}, " +
                "columnNames=Optional[[ccc]]}";
        assertEquals(actual, expected);
    }

    @Test
    public void testHashCode() {
        NodeLocation anotherLocation = new NodeLocation(1, 1);
        WithQuery anotherWithQuery = new WithQuery(anotherLocation, name, query, Optional.of(columnNames));
        assertEquals(withQuery.hashCode(), anotherWithQuery.hashCode());
    }

    @Test
    public void testEquals() {
        WithQuery withQueryCopy = withQuery;
        assertTrue(withQuery.equals(withQueryCopy));
        assertFalse(withQuery.equals(null));
        assertFalse(withQuery.equals(query));

        NodeLocation sameLocation = new NodeLocation(1, 1);
        WithQuery sameWithQuery = new WithQuery(sameLocation, name, query, Optional.of(columnNames));
        assertTrue(withQueryCopy.equals(sameWithQuery));

        IdentifierNode diffName = identifier("bbb");
        WithQuery withQueryDiffName = new WithQuery(sameLocation, diffName, query, Optional.of(columnNames));
        assertFalse(withQuery.equals(withQueryDiffName));

        Query diffQuery = simpleQuery(selectList(identifier("ccc")));
        WithQuery withQueryDiffQuery = new WithQuery(sameLocation, name, diffQuery, Optional.of(columnNames));
        assertFalse(withQuery.equals(withQueryDiffQuery));

        List<IdentifierNode> diffColumnNames = singletonList(identifier("ddd"));
        WithQuery withQueryDiffColumnNames = new WithQuery(sameLocation, name, query, Optional.of(diffColumnNames));
        assertFalse(withQuery.equals(withQueryDiffColumnNames));
    }
}
