/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProcessedQueryTest {
    @Test
    public void testGetter() {
        // coverage
        ProcessedQuery processedQuery = new ProcessedQuery();
        Assert.assertNotNull(processedQuery.getSubExpressionNodes());
        Assert.assertNotNull(processedQuery.getSuperAggregateNodes());
    }
}
