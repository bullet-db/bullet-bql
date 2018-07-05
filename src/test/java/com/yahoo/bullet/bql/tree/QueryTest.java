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
import static com.yahoo.bullet.bql.util.QueryUtil.simpleOrderBy;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleQuery;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleQuerySpecification;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleWith;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class QueryTest {
    private Query query;
    private QueryBody body;

    @BeforeClass
    public void setUp() {
        body = simpleQuerySpecification(selectList(identifier("aaa")));
        query = simpleQuery(body);
    }

    @Test
    public void testGetChildren() {
        List<Node> actual = query.getChildren();
        assertEquals(actual, singletonList(body));
    }

    @Test
    public void testEquals() {
        assertFalse(query.equals(null));
        assertFalse(query.equals(body));

        With diffWith = simpleWith();
        Query queryDiffWith = new Query(Optional.of(diffWith), body, Optional.empty(), Optional.empty());
        assertFalse(query.equals(queryDiffWith));

        OrderBy diffOrderBy = simpleOrderBy();
        Query queryDiffOrderBy = new Query(Optional.empty(), body, Optional.of(diffOrderBy), Optional.empty());
        assertFalse(query.equals(queryDiffOrderBy));

        String diffLimit = "10";
        Query queryDiffLimit = new Query(Optional.empty(), body, Optional.empty(), Optional.of(diffLimit));
        assertFalse(query.equals(queryDiffLimit));
    }
}
