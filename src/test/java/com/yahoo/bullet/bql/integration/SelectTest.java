/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.field;

public class SelectTest extends IntegrationTest {
    @Test
    public void testSelectAll() {
        build("SELECT * FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertNull(query.getFilter());
        Assert.assertEquals(query.getAggregation().getSize(), defaultSize);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertEquals(query.getDuration(), defaultDuration);
    }

    @Test
    public void testWhere() {
        build("SELECT * FROM STREAM() WHERE b");
        Assert.assertEquals(query.getFilter(), field("b", Type.BOOLEAN));
    }

    @Test
    public void testWhereCastable() {
        build("SELECT * FROM STREAM() WHERE abc");
        Assert.assertEquals(query.getFilter(), field("abc", Type.INTEGER));
    }

    @Test
    public void testWhereNotCastable() {
        build("SELECT * FROM STREAM() WHERE aaa");
        Assert.assertEquals(errors.get(0).getError(), "1:30: WHERE clause cannot be casted to BOOLEAN: aaa.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testWhereAggregatesNotAllowed() {
        build("SELECT AVG(abc) FROM STREAM() WHERE AVG(abc) >= 5");
        Assert.assertEquals(errors.get(0).getError(), "WHERE clause cannot contain aggregates.");
    }

    @Test
    public void testWhereIgnoresAlias() {
        build("SELECT abc AS def FROM STREAM() WHERE abc");
        Assert.assertEquals(query.getFilter(), field("abc", Type.INTEGER));
    }

    @Test
    public void testWhereNotSelected() {
        build("SELECT def FROM STREAM() WHERE abc");
        Assert.assertEquals(query.getFilter(), field("abc", Type.INTEGER));
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("def", field("def", Type.FLOAT))));
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.NO_COPY);
    }

    @Test
    public void testLimit() {
        build("SELECT * FROM STREAM() LIMIT 10");
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
    }

    @Test
    public void testLimitZeroNotAllowed() {
        build("SELECT * FROM STREAM() LIMIT 0");
        Assert.assertEquals(errors.get(0).getError(), "LIMIT clause must be positive.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testDuration() {
        build("SELECT * FROM STREAM(2000, TIME)");
        Assert.assertEquals(query.getDuration(), (Long) 2000L);
    }

    @Test
    public void testMaxDuration() {
        build("SELECT * FROM STREAM(MAX, TIME)");
        Assert.assertEquals(query.getDuration(), (Long) Long.MAX_VALUE);
    }

    @Test
    public void testSelectFieldWithType() {
        build("SELECT abc : LIST[MAP[STRING]] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0).getName(), "abc");

        FieldExpression field = (FieldExpression) query.getProjection().getFields().get(0).getValue();
        Assert.assertEquals(field.getType(), Type.STRING_MAP_LIST);
    }

    @Test
    public void testSelectStringValue() {
        build("SELECT 'abc' FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0).getName(), "'abc'");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.NO_COPY);

        ValueExpression value = (ValueExpression) query.getProjection().getFields().get(0).getValue();
        Assert.assertEquals(value.getValue(), "abc");
    }

    @Test
    public void testTooManyQueryTypes() {
        build("SELECT * FROM STREAM() GROUP BY abc");
        //Assert.assertTrue(errors.get(0).getError().startsWith("Query does not match exactly one query type: "));
        Assert.assertEquals(errors.get(0).getError(), "Query consists of multiple aggregation types.");
        Assert.assertEquals(errors.size(), 1);
    }
}
