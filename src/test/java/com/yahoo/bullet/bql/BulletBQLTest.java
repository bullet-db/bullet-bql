/*
 *  Copyright 2018, Oath Inc.
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

        Assert.assertTrue(content.startsWith("{\"projection\":{\"type\":\"PASS_THROUGH\"},\"aggregation\":{\"size\":500,\"type\":\"RAW\"},\"window\":{},\"duration\":9223372036854775807}\n"));
        Assert.assertTrue(content.contains("error: Query does not match exactly one query type"));
    }

    @Test
    public void testConstructor() {
        // coverage
        new BulletBQL();
    }
}
