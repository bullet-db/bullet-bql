/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import com.yahoo.bullet.query.Window;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WindowTest extends IntegrationTest {
    @Test
    public void testWindowEveryAll() {
        build("SELECT COUNT(*) FROM STREAM() WINDOWING EVERY(5000, TIME, ALL)");
        Assert.assertEquals(query.getWindow().getEmitEvery(), (Integer) 5000);
        Assert.assertEquals(query.getWindow().getEmitType(), Window.Unit.TIME);
        Assert.assertEquals(query.getWindow().getIncludeType(), Window.Unit.ALL);
    }

    @Test
    public void testWindowEveryFirst() {
        build("SELECT * FROM STREAM() WINDOWING EVERY(1, RECORD, FIRST, 1, RECORD)");
        Assert.assertEquals(query.getWindow().getEmitEvery(), (Integer) 1);
        Assert.assertEquals(query.getWindow().getEmitType(), Window.Unit.RECORD);
        Assert.assertEquals(query.getWindow().getIncludeFirst(), (Integer) 1);
        Assert.assertEquals(query.getWindow().getIncludeType(), Window.Unit.RECORD);
    }

    @Test
    public void testWindowTumbling() {
        build("SELECT * FROM STREAM() WINDOWING TUMBLING(5000, TIME)");
        Assert.assertEquals(query.getWindow().getEmitEvery(), (Integer) 5000);
        Assert.assertEquals(query.getWindow().getEmitType(), Window.Unit.TIME);
        Assert.assertNull(query.getWindow().getIncludeType());
    }
}
