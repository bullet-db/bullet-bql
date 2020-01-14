/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import org.junit.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.testng.Assert.assertEquals;

public class BulletBQLTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @BeforeClass
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    @AfterClass
    public void restoreStreams() {
        System.setOut(originalOut);
    }

    @Test
    public void testMain() throws Exception {
        /*
        String[] args = new String[]{"SELECT ddd FROM STREAM(2000, TIME) WINDOWING(EVERY, 3000, TIME, FIRST, 3000, TIME) LIMIT 5"};
        BulletBQL.main(args);
        String printOut = outContent.toString();
        assertEquals(printOut, "\n############################## Bullet Query ##############################\n"
                               + "\n{\"projection\":{\"fields\":{\"ddd\":\"ddd\"}},\"aggregation\":{\"size\":5,\"type\":\"RAW\"},\"window\":{\"emit\":{\"type\":\"TIME\",\"every\":3000},\"include\":{\"type\":\"TIME\",\"first\":3000}},\"duration\":2000}\n"
                               + "\n##########################################################################\n\n");
       */
    }
}
