/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests that cover any instance of unknowns i.e. verify that type-checking errors propagate but don't create more error messages
 */
public class SchemaTest extends IntegrationTest {
    @Test
    public void testFieldUnknown() {
        // coverage
        build("SELECT AVG(foo) AS bar FROM STREAM() ORDER BY bar[0]");
        Assert.assertEquals(errors.get(0).getError(), "1:12: The field foo does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testCountDistinctUnknown() {
        // coverage
        build("SELECT COUNT(DISTINCT foo) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:23: The field foo does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testTopKUnknown() {
        // coverage
        build("SELECT TOP(10, foo) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:16: The field foo does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testDistributionUnknown() {
        build("SELECT QUANTILE(foo, LINEAR, 11) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:17: The field foo does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testUnknowns() {
        // coverage
        build("SELECT [(SIZEIS(CAST(IF(foo IS NOT NULL, 5, 10) AS STRING), 10)) + 5], bar + foo, 5 + car FROM STREAM() WHERE foo");
        Assert.assertEquals(errors.get(0).getError(), "1:111: The field foo does not exist in the schema.");
        Assert.assertEquals(errors.get(1).getError(), "1:72: The field bar does not exist in the schema.");
        Assert.assertEquals(errors.get(2).getError(), "1:87: The field car does not exist in the schema.");
        Assert.assertEquals(errors.size(), 3);
    }
}
