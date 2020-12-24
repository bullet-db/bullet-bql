/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.bql.parser.BQLParser;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.typesystem.Type;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Collections;

public class BulletQueryBuilderTest {
    private BulletQueryBuilder builder;

    @BeforeMethod
    public void setup() {
        builder = new BulletQueryBuilder(new BulletConfig());
    }

    @Test
    public void testEmptyBQLString() {
        BQLResult result = builder.buildQuery(null);
        Assert.assertTrue(result.hasErrors());
        Assert.assertEquals(result.getErrors().size(), 1);
        Assert.assertEquals(result.getErrors().get(0).getError(), "The given BQL query is empty.");
        Assert.assertEquals(result.getErrors().get(0).getResolutions(), Collections.singletonList("Please specify a non-empty query."));

        result = builder.buildQuery("");
        Assert.assertTrue(result.hasErrors());
        Assert.assertEquals(result.getErrors().size(), 1);
        Assert.assertEquals(result.getErrors().get(0).getError(), "The given BQL query is empty.");
        Assert.assertEquals(result.getErrors().get(0).getResolutions(), Collections.singletonList("Please specify a non-empty query."));
    }

    @Test
    public void testBQLStringMaxLength() {
        BQLConfig config = new BQLConfig();
        config.set(BQLConfig.BQL_MAX_QUERY_LENGTH, 10);
        config.validate();

        builder = new BulletQueryBuilder(config);

        BQLResult result = builder.buildQuery("SELECT * FROM STREAM()");
        Assert.assertTrue(result.hasErrors());
        Assert.assertEquals(result.getErrors().size(), 1);
        Assert.assertEquals(result.getErrors().get(0).getError(), "The given BQL string is too long. (22 characters)");
        Assert.assertEquals(result.getErrors().get(0).getResolutions(), Collections.singletonList("Please reduce the length of the query to at most 10 characters."));
    }

    @Test
    public void testFormattedBQLStringInResult() {
        BQLResult result = builder.buildQuery("select * from stream();");
        Assert.assertFalse(result.hasErrors());
        Assert.assertNotNull(result.getQuery());
        Assert.assertEquals(result.getBql(), "SELECT * FROM STREAM()");
    }

    @Test
    public void testBQLNoSchema() {
        BQLResult result = builder.buildQuery("SELECT foo FROM STREAM()");
        Assert.assertFalse(result.hasErrors());
        Assert.assertNotNull(result.getQuery());
        Assert.assertEquals(result.getBql(), "SELECT foo FROM STREAM()");

        Expression expression = result.getQuery().getProjection().getFields().get(0).getValue();
        Assert.assertEquals(expression.getType(), Type.UNKNOWN);
    }

    @Test
    public void testParsingException() {
        BQLResult result = builder.buildQuery("not a valid query");
        Assert.assertTrue(result.hasErrors());
        Assert.assertEquals(result.getErrors().size(), 1);
        Assert.assertEquals(result.getErrors().get(0).getError(), "1:1: missing 'SELECT' at 'not'");
        Assert.assertEquals(result.getErrors().get(0).getResolutions(), Collections.singletonList("This is a parsing error."));
    }

    @Test
    public void testMultiLineParsingException() {
        BQLResult result = builder.buildQuery("SELECT * FROM STREAM();\n\n\n ;");
        Assert.assertTrue(result.hasErrors());
        Assert.assertEquals(result.getErrors().size(), 1);
        Assert.assertEquals(result.getErrors().get(0).getError(), "4:2: extraneous input ';' expecting <EOF>");
        Assert.assertEquals(result.getErrors().get(0).getResolutions(), Collections.singletonList("This is a parsing error."));
    }

    @Test
    public void testBulletException() {
        BQLResult result = builder.buildQuery("SELECT QUANTILE(abc, LINEAR, 0) FROM STREAM()");
        Assert.assertTrue(result.hasErrors());
        Assert.assertEquals(result.getErrors().size(), 1);
        Assert.assertEquals(result.getErrors().get(0).getError(), "If specifying the distribution by the number of points, the number must be positive.");
        Assert.assertEquals(result.getErrors().get(0).getResolutions(), Collections.singletonList("Please specify a positive number."));
    }

    @Test
    public void testExceptionCatchAll() throws Exception {
        // Catch-all is only necessary in the case of a programming error, so this cannot happen normally
        BQLParser mockParser = Mockito.mock(BQLParser.class);
        Mockito.when(mockParser.createQueryNode(Mockito.anyString())).thenThrow(new NullPointerException());

        Field field = BulletQueryBuilder.class.getDeclaredField("bqlParser");
        field.setAccessible(true);
        field.set(builder, mockParser);

        BQLResult result = builder.buildQuery("SELECT * FROM STREAM()");
        Assert.assertTrue(result.hasErrors());
        Assert.assertEquals(result.getErrors().size(), 1);
        Assert.assertEquals(result.getErrors().get(0).getError(), "null");
        Assert.assertEquals(result.getErrors().get(0).getResolutions(), Collections.singletonList("This is an application error and not a user error."));
    }
}
