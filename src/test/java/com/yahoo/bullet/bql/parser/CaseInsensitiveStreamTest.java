/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.parser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CaseInsensitiveStreamTest {
    private final String bql = "SELECT * FROM STREAM()";

    @Test
    public void testGetSourceName() {
        // coverage
        Assert.assertEquals(new CaseInsensitiveStream(new ANTLRInputStream(bql)).getSourceName(), "<unknown>");
    }
}
