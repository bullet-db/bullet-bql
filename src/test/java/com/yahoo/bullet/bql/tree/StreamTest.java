/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class StreamTest {
    private StreamNode stream;

    @BeforeClass
    public void setUp() {
        stream = new StreamNode(Optional.of("10"), Optional.of("20"));
    }

    @Test
    public void testGetChildren() {
        assertEquals(stream.getChildren(), emptyList());
    }

    @Test
    public void testEquals() {
        assertFalse(stream.equals(null));
        assertFalse(stream.equals(identifier("aaa")));

        StreamNode streamDiffTimeDuration = new StreamNode(Optional.of("20"), Optional.of("20"));
        assertFalse(stream.equals(streamDiffTimeDuration));
    }
}
