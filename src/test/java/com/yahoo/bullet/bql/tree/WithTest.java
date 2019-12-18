/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static com.yahoo.bullet.bql.util.QueryUtil.selectList;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleQuery;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleWithQuery;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class WithTest {
    private NodeLocation location;
    private boolean recursive;
    private List<WithQuery> queries;
    private With with;

    @BeforeClass
    public void setUp() {
        location = new NodeLocation(1, 1);
        recursive = false;
        queries = singletonList(simpleWithQuery());
        with = new With(location, recursive, queries);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "queries is empty")
    public void testConstructorEmptyQuery() {
        List<WithQuery> emptyQuery = Collections.emptyList();
        new With(recursive, emptyQuery);
    }

    @Test
    public void testIsRecursive() {
        assertEquals(with.isRecursive(), recursive);
    }

    @Test
    public void testGetQueries() {
        assertEquals(with.getQueries(), queries);
    }

    @Test
    public void testGetChildren() {
        assertEquals(with.getChildren(), queries);
    }

    @Test
    public void testEquals() {
        With withCopy = with;
        assertTrue(with.equals(withCopy));
        assertFalse(with.equals(null));
        assertFalse(with.equals(queries));

        NodeLocation anotherLocation = new NodeLocation(1, 1);
        With sameWith = new With(anotherLocation, recursive, queries);
        assertTrue(with.equals(sameWith));

        With withDiffRecursive = new With(location, !recursive, queries);
        assertFalse(with.equals(withDiffRecursive));

        WithQuery diffWithQuery = new WithQuery(identifier("bbb"), simpleQuery(selectList(identifier("bbb"))), Optional.empty());
        List<WithQuery> diffQueries = singletonList(diffWithQuery);
        With withDiffQueries = new With(recursive, diffQueries);
        assertFalse(with.equals(withDiffQueries));
    }

    @Test
    public void testHashCode() {
        NodeLocation anotherLocation = new NodeLocation(1, 1);
        With sameWith = new With(anotherLocation, recursive, queries);
        assertEquals(with.hashCode(), sameWith.hashCode());
    }

    @Test
    public void testToString() {
        String actual = with.toString();
        String expected = "With{" +
                "recursive=false, " +
                "queries=[" +
                "WithQuery{" +
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
                "columnNames=Optional.empty}]}";
        assertEquals(actual, expected);
    }
}
