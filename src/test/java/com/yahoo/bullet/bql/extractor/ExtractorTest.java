/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.QueryValidator;
import com.yahoo.bullet.bql.query.TypeChecker;
import org.testng.annotations.Test;

public class ExtractorTest {
    @Test
    public void testConstructors() {
        // coverage
        new AggregationExtractor();
        new ComputationExtractor();
        new PostAggregationExtractor();
        new ProjectionExtractor();
        new TransientFieldExtractor();
        new WindowExtractor();
        new QueryValidator();
        new TypeChecker();
    }
}
