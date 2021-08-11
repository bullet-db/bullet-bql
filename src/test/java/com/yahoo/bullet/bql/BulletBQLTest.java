/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;


public class BulletBQLTest {
    private final InputStream systemIn = System.in;
    private final PrintStream systemOut = System.out;

    @AfterClass
    public void restoreStreams() {
        System.setIn(systemIn);
        System.setOut(systemOut);
    }

    @Test
    public void testMain() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        System.setOut(new PrintStream(out));

        String bql = "SELECT * FROM STREAM()\nSELECT * FROM STREAM() GROUP BY 1\n...\n\n";

        System.setIn(new ByteArrayInputStream(bql.getBytes()));

        BulletBQL.main(null);

        String content = out.toString();

        Assert.assertTrue(content.startsWith("{tableFunction: null, projection: {fields: null, type: PASS_THROUGH}, filter: null, aggregation: {size: 500, type: RAW}, postAggregations: null, window: {emitEvery: null, emitType: null, includeType: null, includeFirst: null}, duration: 9223372036854775807, postQuery: null}\n"));
        Assert.assertTrue(content.contains("error: Query consists of multiple aggregation types."));
    }

    @Test
    public void testConstructor() {
        // coverage
        new BulletBQL();
    }
}
