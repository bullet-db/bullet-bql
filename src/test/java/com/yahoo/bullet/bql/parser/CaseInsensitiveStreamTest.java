/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.parser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class CaseInsensitiveStreamTest {
    @Test
    public void testGetSourceName() {
        String bql = "SELECT aaa FROM STREAM(2000, TIME)";
        CaseInsensitiveStream stream = new CaseInsensitiveStream(new ANTLRInputStream(bql));
        assertEquals(stream.getSourceName(), "<unknown>");
    }
}
