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
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TopKTest {
    private List<Expression> columns;
    private TopK topK;

    @BeforeClass
    public void setUp() {
        columns = singletonList(identifier("aaa"));
        topK = new TopK(columns, (long) 10, Optional.of((long) 5));
    }

    @Test
    public void testGetChildren() {
        assertEquals(topK.getChildren(), emptyList());
    }

    @Test
    public void testEquals() {
        TopK copy = topK;
        assertTrue(topK.equals(copy));
        assertFalse(topK.equals(null));
        assertFalse(topK.equals(columns));

        TopK topKDiffColumns = new TopK(singletonList(identifier("bbb")), (long) 10, Optional.of((long) 5));
        assertFalse(topK.equals(topKDiffColumns));

        TopK topKDiffSize = new TopK(columns, (long) 5, Optional.of((long) 5));
        assertFalse(topK.equals(topKDiffSize));

        TopK topKDiffThreshold = new TopK(columns, (long) 10, Optional.of((long) 10));
        assertFalse(topK.equals(topKDiffThreshold));
    }
}
