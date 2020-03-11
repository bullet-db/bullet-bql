/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.extractor.AggregationExtractor;
import com.yahoo.bullet.bql.extractor.ComputationExtractor;
import com.yahoo.bullet.bql.extractor.PostAggregationExtractor;
import com.yahoo.bullet.bql.extractor.ProjectionExtractor;
import com.yahoo.bullet.bql.extractor.TransientFieldExtractor;
import com.yahoo.bullet.bql.extractor.WindowExtractor;
import org.testng.Assert;
import org.testng.annotations.Test;

// Has miscellaneous tests for now
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

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "This method should not be called\\.")
    public void testVisitNode() {
        // coverage
        new QueryProcessor().visitNode(null, null);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "This method should not be called\\.")
    public void testVisitExpression() {
        // coverage
        new QueryProcessor().visitExpression(null, null);
    }

    @Test
    public void testGetter() {
        // coverage
        ProcessedQuery processedQuery = new ProcessedQuery();
        Assert.assertNotNull(processedQuery.getSubExpressionNodes());
        Assert.assertNotNull(processedQuery.getSuperAggregateNodes());
    }
/*
    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = ".*Literal missing its type\\.")
    public void testLiteral() {
        new ExpressionValidator(null, Collections.emptyMap()).visitLiteral(null, null);
    }
*/
}
