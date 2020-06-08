/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.parser;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ParsingExceptionTest {
    @Test
    public void testGetMessage() {
        Assert.assertEquals(new ParsingException("exception").getMessage(), "1:1: exception");
        Assert.assertEquals(new ParsingException("another exception", null, 2, 0).getMessage(), "2:1: another exception");
    }
}
