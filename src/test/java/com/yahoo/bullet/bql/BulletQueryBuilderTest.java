/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.common.BulletConfig;

import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Culling;
import com.yahoo.bullet.parsing.OrderBy;
import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.expressions.BinaryExpression;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.parsing.expressions.Operation;
import com.yahoo.bullet.parsing.expressions.ValueExpression;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.yahoo.bullet.parsing.Projection.Field;

public class BulletQueryBuilderTest {
    private BulletQueryBuilder builder;
    private Query query;

    @BeforeClass
    public void setup() {
        builder = new BulletQueryBuilder(new BulletConfig());
    }

    private void build(String bql) {
        query = builder.buildQuery(bql);
    }

    @Test
    public void testRawAll() {
        build("SELECT * FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertNull(query.getFilter());
        Assert.assertNull(query.getAggregation().getSize());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.RAW);
        Assert.assertNull(query.getDuration());
    }

    @Test
    public void testWhere() {
        build("SELECT * FROM STREAM() WHERE abc");
        Assert.assertEquals(query.getFilter(), new FieldExpression("abc"));
    }

    @Test
    public void testLimit() {
        build("SELECT * FROM STREAM() LIMIT 10");
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.RAW);
    }

    @Test
    public void testDuration() {
        build("SELECT * FROM STREAM(2000, TIME)");
        Assert.assertEquals(query.getDuration(), (Long) 2000L);
    }

    @Test
    public void testRaw() {
        build("SELECT abc FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", new FieldExpression("abc")));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.RAW);
    }

    @Test
    public void testRawWithUnnecessaryParentheses() {
        build("SELECT ((abc)) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", new FieldExpression("abc")));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.RAW);
    }

    @Test
    public void testRawAlias() {
        build("SELECT abc AS def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("def", new FieldExpression("abc")));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.RAW);
    }

    @Test
    public void testQuotedRawAlias() {
        build("SELECT \"abc\" AS \"def\" FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("def", new FieldExpression("abc")));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.RAW);
    }

    @Test
    public void testRawAllWithField() {
        build("SELECT *, abc FROM STREAM()");
        Assert.assertNull(query.getProjection());
    }

    @Test
    public void testRawAllWithComputation() {
        build("SELECT *, abc + 5 FROM STREAM()");
        Assert.assertNotNull(query.getProjection());
        Assert.assertNull(query.getProjection().getFields());
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregation.Type.COMPUTATION);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getProjection().getFields().size(), 1);
        Assert.assertEquals(computation.getProjection().getFields().get(0), new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                      new ValueExpression(5),
                                                                                                                      Operation.ADD)));
    }

    @Test
    public void testRawAllWithSubField() {
        build("SELECT *, abc[0] FROM STREAM()");
        Assert.assertNotNull(query.getProjection());
        Assert.assertNull(query.getProjection().getFields());
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregation.Type.COMPUTATION);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getProjection().getFields().size(), 1);
        Assert.assertEquals(computation.getProjection().getFields().get(0), new Field("abc[0]", new FieldExpression("abc", 0)));
    }

    @Test
    public void testRawAllWithOrderByField() {
        build("SELECT * FROM STREAM() ORDER BY abc");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregation.Type.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc");
    }

    @Test
    public void testRawAllWithOrderByComputation() {
        build("SELECT * FROM STREAM() ORDER BY abc + 5");
        Assert.assertNotNull(query.getProjection());
        Assert.assertNull(query.getProjection().getFields());
        Assert.assertEquals(query.getPostAggregations().size(), 3);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregation.Type.COMPUTATION);
        Assert.assertEquals(query.getPostAggregations().get(1).getType(), PostAggregation.Type.ORDER_BY);
        Assert.assertEquals(query.getPostAggregations().get(2).getType(), PostAggregation.Type.CULLING);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getProjection().getFields().size(), 1);
        Assert.assertEquals(computation.getProjection().getFields().get(0), new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                      new ValueExpression(5),
                                                                                                                      Operation.ADD)));

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(1);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc + 5");

        Culling culling = (Culling) query.getPostAggregations().get(2);

        Assert.assertEquals(culling.getTransientFields().size(), 1);
        Assert.assertTrue(culling.getTransientFields().contains("abc + 5"));
    }

    @Test
    public void testRawWithOrderBySameField() {
        build("SELECT abc FROM STREAM() ORDER BY abc");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", new FieldExpression("abc")));
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregation.Type.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc");
    }

    @Test
    public void testRawWithOrderByDifferentField() {
        build("SELECT abc FROM STREAM() ORDER BY def");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", new FieldExpression("abc")));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("def", new FieldExpression("def")));
        Assert.assertEquals(query.getPostAggregations().size(), 2);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregation.Type.ORDER_BY);
        Assert.assertEquals(query.getPostAggregations().get(1).getType(), PostAggregation.Type.CULLING);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "def");

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields().size(), 1);
        Assert.assertTrue(culling.getTransientFields().contains("def"));
    }

    @Test
    public void testRawWithOrderByMultiple() {
        build("SELECT abc FROM STREAM() ORDER BY abc + 5, def DESC");
        Assert.assertEquals(query.getProjection().getFields().size(), 3);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", new FieldExpression("abc")));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                new ValueExpression(5),
                                                                                                                Operation.ADD)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("def", new FieldExpression("def")));
        Assert.assertEquals(query.getPostAggregations().size(), 2);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregation.Type.ORDER_BY);
        Assert.assertEquals(query.getPostAggregations().get(1).getType(), PostAggregation.Type.CULLING);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 2);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc + 5");
        Assert.assertEquals(orderBy.getFields().get(0).getDirection(), OrderBy.Direction.ASC);
        Assert.assertEquals(orderBy.getFields().get(1).getField(), "def");
        Assert.assertEquals(orderBy.getFields().get(1).getDirection(), OrderBy.Direction.DESC);

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields().size(), 2);
        Assert.assertTrue(culling.getTransientFields().contains("abc + 5"));
        Assert.assertTrue(culling.getTransientFields().contains("def"));
    }

    @Test
    public void testSelectFieldWithType() {
        build("SELECT abc : LIST[MAP[STRING]] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0).getName(), "abc");

        FieldExpression field = (FieldExpression) query.getProjection().getFields().get(0).getValue();
        Assert.assertEquals(field.getType(), Type.LISTOFMAP);
        Assert.assertEquals(field.getPrimitiveType(), Type.STRING);
    }

    @Test
    public void testSelectStringValue() {
        build("SELECT 'abc' FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0).getName(), "'abc'");

        ValueExpression value = (ValueExpression) query.getProjection().getFields().get(0).getValue();
        Assert.assertEquals(value.getValue(), "abc");
    }

    @Test
    public void testRawAliasWithOrderBy() {
        build("SELECT abc AS def FROM STREAM() ORDER BY abc");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("def", new FieldExpression("abc"))));
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregation.Type.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "def");
    }

    @Test
    public void testRawAliasWithOrderByAlias() {
        build("SELECT abc AS def FROM STREAM() ORDER BY def");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("def", new FieldExpression("abc"))));
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregation.Type.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "def");
    }

    @Test
    public void testRawAliasWithOrderByPrioritizeSimpleAlias1() {
        build("SELECT abc AS def, def AS abc FROM STREAM() ORDER BY abc");
        Assert.assertEquals(query.getProjection().getFields(), Arrays.asList(new Field("def", new FieldExpression("abc")),
                                                                             new Field("abc", new FieldExpression("def"))));
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregation.Type.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc");
    }

    @Test
    public void testRawAliasWithOrderByPrioritizeSimpleAlias2() {
        build("SELECT abc AS def, def AS abc FROM STREAM() ORDER BY def");
        Assert.assertEquals(query.getProjection().getFields(), Arrays.asList(new Field("def", new FieldExpression("abc")),
                                                                             new Field("abc", new FieldExpression("def"))));
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregation.Type.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "def");
    }

    @Test
    public void testRawAliasWithOrderByNoNestedAlias() {
        build("SELECT abc AS def, def AS abc FROM STREAM() ORDER BY abc + 5");
        Assert.assertEquals(query.getProjection().getFields(), Arrays.asList(new Field("def", new FieldExpression("abc")),
                                                                             new Field("abc", new FieldExpression("def")),
                                                                             new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                       new ValueExpression(5),
                                                                                                                       Operation.ADD))));
        Assert.assertEquals(query.getPostAggregations().size(), 2);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregation.Type.ORDER_BY);
        Assert.assertEquals(query.getPostAggregations().get(1).getType(), PostAggregation.Type.CULLING);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc + 5");

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("abc + 5"));
    }
}
