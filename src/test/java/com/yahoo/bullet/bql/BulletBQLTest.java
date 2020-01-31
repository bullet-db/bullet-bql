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
    private final PrintStream systemErr = System.err;

    @AfterClass
    public void restoreStreams() {
        System.setIn(systemIn);
        System.setOut(systemOut);
        System.setErr(systemErr);
    }

    @Test
    public void testMain() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        System.setOut(new PrintStream(out));
        System.setErr(new PrintStream(err));

        String bql = "SELECT * FROM STREAM()\n...\n\n";

        System.setIn(new ByteArrayInputStream(bql.getBytes()));

        BulletBQL.main(null);

        Assert.assertEquals(out.toString(), "{\"aggregation\":{\"size\":500,\"type\":\"RAW\"},\"duration\":9223372036854775807}\nOptional.empty\n");
        Assert.assertTrue(err.toString().contains("ParsingException"));
    }

    @Test
    public void testConstructor() {
        // coverage
        new BulletBQL();
    }
}
