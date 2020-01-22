/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import org.junit.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import static org.testng.Assert.assertEquals;

public class BulletBQLTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final InputStream systemIn = System.in;
    private final PrintStream systemOut = System.out;

    @BeforeClass
    public void setup() {
        System.setOut(new PrintStream(outContent));
    }

    @AfterClass
    public void restoreStreams() {
        System.setIn(systemIn);
        System.setOut(systemOut);
    }

    @Test
    public void testMain() {
        String bql = "SELECT * FROM STREAM()\n\n";

        System.setIn(new ByteArrayInputStream(bql.getBytes()));

        BulletBQL.main(null);

        String printOut = outContent.toString();
        assertEquals(printOut, "{\"aggregation\":{\"size\":500,\"type\":\"RAW\"},\"duration\":9223372036854775807}\n" +
                               "Optional.empty\n");
    }
}
