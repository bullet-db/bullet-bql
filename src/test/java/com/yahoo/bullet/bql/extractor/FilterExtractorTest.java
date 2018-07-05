/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.tree.Query;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static com.yahoo.bullet.bql.util.QueryUtil.selectList;
import static com.yahoo.bullet.bql.util.QueryUtil.simpleQuery;
import static org.testng.Assert.assertNull;

public class FilterExtractorTest {
    private FilterExtractor extractor;

    @BeforeClass
    public void setUp() {
        extractor = new FilterExtractor();
    }

    @Test
    public void testExtractFilterFromQueryWithoutFilter() {
        Query query = simpleQuery(selectList(identifier("aaa")));
        assertNull(extractor.extractFilter(query).get(0));
    }
}
