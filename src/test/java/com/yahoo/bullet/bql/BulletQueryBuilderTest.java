/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.aggregations.CountDistinct;
import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.TopK;
import com.yahoo.bullet.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.bql.parser.ParsingException;
import com.yahoo.bullet.common.BulletConfig;

import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Culling;
import com.yahoo.bullet.parsing.Having;
import com.yahoo.bullet.parsing.OrderBy;
import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.parsing.expressions.BinaryExpression;
import com.yahoo.bullet.parsing.expressions.CastExpression;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.parsing.expressions.ListExpression;
import com.yahoo.bullet.parsing.expressions.NAryExpression;
import com.yahoo.bullet.parsing.expressions.Operation;
import com.yahoo.bullet.parsing.expressions.UnaryExpression;
import com.yahoo.bullet.parsing.expressions.ValueExpression;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    public void testWhereIgnoresAlias() {
        build("SELECT abc AS def FROM STREAM() WHERE abc");
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
    public void testMaxDuration() {
        build("SELECT * FROM STREAM(MAX, TIME)");
        Assert.assertEquals(query.getDuration(), (Long) Long.MAX_VALUE);
    }

    @Test
    public void testWindowEveryAll() {
        build("SELECT * FROM STREAM() WINDOWING EVERY(5000, TIME, ALL)");
        Assert.assertEquals(query.getWindow().getEmit().get(Window.EMIT_EVERY_FIELD), 5000L);
        Assert.assertEquals(query.getWindow().getEmit().get(Window.TYPE_FIELD), "TIME");
        Assert.assertEquals(query.getWindow().getInclude().get(Window.TYPE_FIELD), "ALL");
    }

    @Test
    public void testWindowEveryFirst() {
        build("SELECT * FROM STREAM() WINDOWING EVERY(1, RECORD, FIRST, 1, RECORD)");
        Assert.assertEquals(query.getWindow().getEmit().get(Window.EMIT_EVERY_FIELD), 1L);
        Assert.assertEquals(query.getWindow().getEmit().get(Window.TYPE_FIELD), "RECORD");
        Assert.assertEquals(query.getWindow().getInclude().get(Window.INCLUDE_FIRST_FIELD), 1L);
        Assert.assertEquals(query.getWindow().getInclude().get(Window.TYPE_FIELD), "RECORD");
    }

    @Test
    public void testWindowTumbling() {
        build("SELECT * FROM STREAM() WINDOWING TUMBLING(5000, TIME)");
        Assert.assertEquals(query.getWindow().getEmit().get(Window.EMIT_EVERY_FIELD), 5000L);
        Assert.assertEquals(query.getWindow().getEmit().get(Window.TYPE_FIELD), "TIME");
        Assert.assertNull(query.getWindow().getInclude());
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

    @Test
    public void testSingleDistinct() {
        build("SELECT DISTINCT abc FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields().size(), 1);
        Assert.assertEquals(query.getAggregation().getFields().get("abc"), "abc");
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertNull(query.getAggregation().getSize());
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testMultipleDistinct() {
        build("SELECT DISTINCT abc, def FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields().size(), 2);
        Assert.assertEquals(query.getAggregation().getFields().get("abc"), "abc");
        Assert.assertEquals(query.getAggregation().getFields().get("def"), "def");
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertNull(query.getAggregation().getSize());
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testDistinctWithAlias() {
        build("SELECT DISTINCT abc AS one, def AS two FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields().size(), 2);
        Assert.assertEquals(query.getAggregation().getFields().get("abc"), "one");
        Assert.assertEquals(query.getAggregation().getFields().get("def"), "two");
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertNull(query.getAggregation().getSize());
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testDistinctWithComputation() {
        build("SELECT DISTINCT abc + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                new ValueExpression(5),
                                                                                                                Operation.ADD)));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields().size(), 1);
        Assert.assertEquals(query.getAggregation().getFields().get("abc + 5"), "abc + 5");
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertNull(query.getAggregation().getSize());
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testDistinctWithOrderBy() {
        build("SELECT DISTINCT abc FROM STREAM() ORDER BY def");
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "def");
    }

    @Test
    public void testDistinctWithOrderByComputation() {
        build("SELECT DISTINCT abc FROM STREAM() ORDER BY def + 5");
        Assert.assertEquals(query.getPostAggregations().size(), 3);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getProjection().getFields().size(), 1);
        Assert.assertEquals(computation.getProjection().getFields().get(0), new Field("def + 5", new BinaryExpression(new FieldExpression("def"),
                                                                                                                      new ValueExpression(5),
                                                                                                                      Operation.ADD)));

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(1);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "def");

        Culling culling = (Culling) query.getPostAggregations().get(2);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("def"));
    }

    @Test
    public void testGroupBy() {
        build("SELECT abc FROM STREAM() GROUP BY abc");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertNull(query.getAggregation().getAttributes());
    }

    @Test
    public void testGroupByWithAlias() {
        build("SELECT abc AS def FROM STREAM() GROUP BY abc");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "def"));
        Assert.assertNull(query.getAggregation().getAttributes());
    }

    @Test
    public void testGroupByComputation() {
        build("SELECT abc + 5 FROM STREAM() GROUP BY abc + 5");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                                   new ValueExpression(5),
                                                                                                                                   Operation.ADD))));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc + 5", "abc + 5"));
        Assert.assertNull(query.getAggregation().getAttributes());
    }

    @Test
    public void testGroupByComputationWithAlias() {
        build("SELECT abc + 5 AS def FROM STREAM() GROUP BY abc + 5");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                                   new ValueExpression(5),
                                                                                                                                   Operation.ADD))));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc + 5", "def"));
        Assert.assertNull(query.getAggregation().getAttributes());
    }

    @Test
    public void testGroupByWithComputationAndOrderBy() {
        build("SELECT abc + 5 FROM STREAM() GROUP BY abc + 5 ORDER BY abc + 5");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                                   new ValueExpression(5),
                                                                                                                                   Operation.ADD))));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc + 5", "abc + 5"));
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc + 5");
    }

    @Test
    public void testGroupByWithComputationAliasAndOrderBy() {
        build("SELECT abc + 5 AS def FROM STREAM() GROUP BY abc + 5 ORDER BY def");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                                   new ValueExpression(5),
                                                                                                                                   Operation.ADD))));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc + 5", "def"));
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "def");
    }

    @Test
    public void testGroupOp() {
        build("SELECT AVG(abc) FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertNull(query.getAggregation().getFields());
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 2);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "abc");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), "AVG");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(abc)");

        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testGroupOpWithGroupBy() {
        build("SELECT AVG(abc) FROM STREAM() GROUP BY abc");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 3);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "abc");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), "AVG");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(abc)");

        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testGroupOpWithComputation() {
        build("SELECT AVG(abc + 5) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                                   new ValueExpression(5),
                                                                                                                                   Operation.ADD))));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertNull(query.getAggregation().getFields());
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 3);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "abc + 5");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), "AVG");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(abc + 5)");

        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testGroupOpAsComputation() {
        build("SELECT AVG(abc) + 5 FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertNull(query.getAggregation().getFields());
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 3);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "abc");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), "AVG");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(abc)");

        Assert.assertEquals(query.getPostAggregations().size(), 2);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getProjection().getFields(), Collections.singletonList(new Field("AVG(abc) + 5", new BinaryExpression(new FieldExpression("AVG(abc)"),
                                                                                                                                              new ValueExpression(5),
                                                                                                                                              Operation.ADD))));

        Culling culling = (Culling) query.getPostAggregations().get(0);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("AVG(abc)"));
    }

    @Test
    public void testGroupByHaving() {
        build("SELECT AVG(abc) FROM STREAM() HAVING AVG(abc) >= 5");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                                   new ValueExpression(5),
                                                                                                                                   Operation.ADD))));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertNull(query.getAggregation().getFields());
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 3);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "abc");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), "AVG");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(abc)");

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Having having = (Having) query.getPostAggregations().get(0);

        Assert.assertEquals(having.getExpression(), new BinaryExpression(new FieldExpression("AVG(abc)"),
                                                                         new ValueExpression(5),
                                                                         Operation.GREATER_THAN_OR_EQUALS));
    }

    @Test
    public void testGroupByHavingIgnoresFieldAlias() {
        build("SELECT abc AS def, AVG(abc) FROM STREAM() GROUP BY abc HAVING abc >= 5");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "def"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 3);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "abc");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), "AVG");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(abc)");

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Having having = (Having) query.getPostAggregations().get(0);

        Assert.assertEquals(having.getExpression(), new BinaryExpression(new FieldExpression("abc"),
                                                                         new ValueExpression(5),
                                                                         Operation.GREATER_THAN_OR_EQUALS));
    }

    @Test
    public void testGroupByHavingUsesAggregateAlias() {
        build("SELECT abc, AVG(abc) AS avg FROM STREAM() GROUP BY abc HAVING AVG(abc) >= 5");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 3);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "abc");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), "AVG");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "avg");

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Having having = (Having) query.getPostAggregations().get(0);

        Assert.assertEquals(having.getExpression(), new BinaryExpression(new FieldExpression("avg"),
                                                                         new ValueExpression(5),
                                                                         Operation.GREATER_THAN_OR_EQUALS));
    }

    @Test
    public void testGroupByHavingUnselectedAggregate() {
        build("SELECT abc FROM STREAM() GROUP BY abc HAVING AVG(def) >= 5");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 3);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "def");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), "AVG");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(def)");

        Assert.assertEquals(query.getPostAggregations().size(), 2);

        Having having = (Having) query.getPostAggregations().get(0);

        Assert.assertEquals(having.getExpression(), new BinaryExpression(new FieldExpression("AVG(def)"),
                                                                         new ValueExpression(5),
                                                                         Operation.GREATER_THAN_OR_EQUALS));

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("AVG(def)"));
    }

    @Test
    public void testGroupByOrderByUnselectedAggregate() {
        build("SELECT abc FROM STREAM() GROUP BY abc ORDER BY AVG(def)");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 3);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "def");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), "AVG");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(def)");

        Assert.assertEquals(query.getPostAggregations().size(), 2);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "AVG(def)");

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("AVG(def)"));
    }

    @Test
    public void testCountDistinct() {
        build("SELECT COUNT(DISTINCT abc) FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.COUNT_DISTINCT);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes(), Collections.singletonMap(CountDistinct.NEW_NAME_FIELD, "COUNT(DISTINCT abc)"));
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testCountDistinctWithComputation() {
        build("SELECT COUNT(DISTINCT abc + 5) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                                   new ValueExpression(5),
                                                                                                                                   Operation.ADD))));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.COUNT_DISTINCT);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc + 5", "abc + 5"));
        Assert.assertEquals(query.getAggregation().getAttributes(), Collections.singletonMap(CountDistinct.NEW_NAME_FIELD, "COUNT(DISTINCT abc + 5)"));
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testCountDistinctAsComputation() {
        build("SELECT COUNT(DISTINCT abc), COUNT(DISTINCT abc) + 5 FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.COUNT_DISTINCT);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes(), Collections.singletonMap(CountDistinct.NEW_NAME_FIELD, "COUNT(DISTINCT abc)"));
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getProjection().getFields(), Collections.singletonList(new Field("COUNT(DISTINCT abc) + 5", new BinaryExpression(new FieldExpression("COUNT(DISTINCT abc)"),
                                                                                                                                                         new ValueExpression(5),
                                                                                                                                                         Operation.ADD))));
    }

    @Test
    public void testCountDistinctAsComputationOnly() {
        build("SELECT COUNT(DISTINCT abc) + 5 FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.COUNT_DISTINCT);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes(), Collections.singletonMap(CountDistinct.NEW_NAME_FIELD, "COUNT(DISTINCT abc)"));
        Assert.assertEquals(query.getPostAggregations().size(), 2);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getProjection().getFields(), Collections.singletonList(new Field("COUNT(DISTINCT abc) + 5", new BinaryExpression(new FieldExpression("COUNT(DISTINCT abc)"),
                                                                                                                                                         new ValueExpression(5),
                                                                                                                                                         Operation.ADD))));

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("COUNT(DISTINCT abc)"));
    }

    @Test
    public void testCountDistinctAliasAsComputation() {
        build("SELECT COUNT(DISTINCT abc) AS count, COUNT(DISTINCT abc) + 5 FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.COUNT_DISTINCT);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes(), Collections.singletonMap(CountDistinct.NEW_NAME_FIELD, "count"));
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getProjection().getFields(), Collections.singletonList(new Field("COUNT(DISTINCT abc) + 5", new BinaryExpression(new FieldExpression("count"),
                                                                                                                                                         new ValueExpression(5),
                                                                                                                                                         Operation.ADD))));
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = ".*Cannot have multiple count distincts\\.")
    public void testMultipleCountDistinct() {
        build("SELECT COUNT(DISTINCT abc), COUNT(DISTINCT def) FROM STREAM()");
    }

    @Test
    public void testQuantileDistribution() {
        build("SELECT QUANTILE(abc, LINEAR, 11) FROM STREAM() LIMIT 20");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.DISTRIBUTION);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 2);
        Assert.assertEquals(query.getAggregation().getAttributes().get(Distribution.TYPE), Distribution.Type.QUANTILE);
        Assert.assertEquals(query.getAggregation().getAttributes().get(Distribution.NUMBER_OF_POINTS), 11L);
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 20);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testPMFDistribution() {
        build("SELECT FREQ(abc, REGION, 2000, 20000, 500) FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.DISTRIBUTION);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes().get(Distribution.TYPE), Distribution.Type.PMF);
        Assert.assertEquals(query.getAggregation().getAttributes().get(Distribution.RANGE_START), 2000.0);
        Assert.assertEquals(query.getAggregation().getAttributes().get(Distribution.RANGE_END), 20000.0);
        Assert.assertEquals(query.getAggregation().getAttributes().get(Distribution.RANGE_INCREMENT), 500.0);
        Assert.assertNull(query.getAggregation().getSize());
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testCDFDistribution() {
        build("SELECT CUMFREQ(abc, MANUAL, 20000, 2000, 15000, 45000) FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.DISTRIBUTION);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes().get(Distribution.TYPE), Distribution.Type.CDF);
        Assert.assertEquals(query.getAggregation().getAttributes().get(Distribution.POINTS), Arrays.asList(20000.0, 2000.0, 15000.0, 45000.0));
        Assert.assertNull(query.getAggregation().getSize());
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testDistributionWithComputation() {
        build("SELECT QUANTILE(abc + 5, LINEAR, 11) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                                   new ValueExpression(5),
                                                                                                                                   Operation.ADD))));
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc + 5", "abc + 5"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 2);
        Assert.assertEquals(query.getAggregation().getAttributes().get(Distribution.TYPE), Distribution.Type.QUANTILE);
        Assert.assertEquals(query.getAggregation().getAttributes().get(Distribution.NUMBER_OF_POINTS), 11L);
        Assert.assertNull(query.getAggregation().getSize());
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopK() {
        build("SELECT TOP(10, abc) FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKWithThreshold() {
        build("SELECT TOP(10, 100, abc) FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes(), Collections.singletonMap(TopK.THRESHOLD_FIELD, 100L));
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKTakesMinLimit1() {
        build("SELECT TOP(10, abc) FROM STREAM() LIMIT 50");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKTakesMinLimit2() {
        build("SELECT TOP(10, abc) FROM STREAM() LIMIT 5");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 5);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKWithMultipleFields() {
        build("SELECT TOP(10, abc, def) FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields().size(), 2);
        Assert.assertEquals(query.getAggregation().getFields().get("abc"), "abc");
        Assert.assertEquals(query.getAggregation().getFields().get("def"), "def");
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKWithComputations() {
        build("SELECT TOP(10, abc, def + 5) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("def + 5", new BinaryExpression(new FieldExpression("def"),
                                                                                                                                   new ValueExpression(5),
                                                                                                                                   Operation.ADD))));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields().size(), 2);
        Assert.assertEquals(query.getAggregation().getFields().get("abc"), "abc");
        Assert.assertEquals(query.getAggregation().getFields().get("def + 5"), "def + 5");
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKWithAdditionalFields() {
        build("SELECT TOP(10, abc, def + 5), abc + 5, count + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("def + 5", new BinaryExpression(new FieldExpression("def"),
                                                                                                                                   new ValueExpression(5),
                                                                                                                                   Operation.ADD))));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields().size(), 2);
        Assert.assertEquals(query.getAggregation().getFields().get("abc"), "abc");
        Assert.assertEquals(query.getAggregation().getFields().get("def + 5"), "def + 5");
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getProjection().getFields().size(), 2);
        Assert.assertEquals(computation.getProjection().getFields().get(0), new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                      new ValueExpression(5),
                                                                                                                      Operation.ADD)));
        Assert.assertEquals(computation.getProjection().getFields().get(1), new Field("count + 5", new BinaryExpression(new FieldExpression("count"),
                                                                                                                        new ValueExpression(5),
                                                                                                                        Operation.ADD)));
    }

    @Test
    public void testTopKAlias() {
        build("SELECT TOP(10, abc) AS top, top + 5 FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes(), Collections.singletonMap(TopK.NEW_NAME_FIELD, "top"));
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getProjection().getFields().size(), 1);
        Assert.assertEquals(computation.getProjection().getFields().get(0), new Field("top + 5", new BinaryExpression(new FieldExpression("top"),
                                                                                                                      new ValueExpression(5),
                                                                                                                      Operation.ADD)));
    }

    @Test
    public void testTopKWithAlias() {
        build("SELECT TOP(10, abc, def), abc AS one, def AS two FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields().size(), 2);
        Assert.assertEquals(query.getAggregation().getFields().get("abc"), "one");
        Assert.assertEquals(query.getAggregation().getFields().get("def"), "two");
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialK() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes(), Collections.singletonMap(TopK.NEW_NAME_FIELD, "COUNT(*)"));
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithMultipleFields() {
        build("SELECT abc, def, COUNT(*) FROM STREAM() GROUP BY abc, def ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields().size(), 2);
        Assert.assertEquals(query.getAggregation().getFields().get("abc"), "abc");
        Assert.assertEquals(query.getAggregation().getFields().get("def"), "def");
        Assert.assertEquals(query.getAggregation().getAttributes(), Collections.singletonMap(TopK.NEW_NAME_FIELD, "COUNT(*)"));
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithHaving() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING COUNT(*) >= 100 ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 2);
        Assert.assertEquals(query.getAggregation().getAttributes().get(TopK.NEW_NAME_FIELD), "COUNT(*)");
        Assert.assertEquals(query.getAggregation().getAttributes().get(TopK.THRESHOLD_FIELD), 100L);
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithAliasField() {
        build("SELECT abc AS def, COUNT(*) FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "def"));
        Assert.assertEquals(query.getAggregation().getAttributes(), Collections.singletonMap(TopK.NEW_NAME_FIELD, "COUNT(*)"));
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithAliasCount1() {
        build("SELECT abc, COUNT(*) AS count FROM STREAM() GROUP BY abc HAVING count >= 50 ORDER BY count DESC LIMIT 10");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 2);
        Assert.assertEquals(query.getAggregation().getAttributes().get(TopK.NEW_NAME_FIELD), "count");
        Assert.assertEquals(query.getAggregation().getAttributes().get(TopK.THRESHOLD_FIELD), 50L);
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithAliasCount2() {
        build("SELECT abc, COUNT(*) AS count FROM STREAM() GROUP BY abc HAVING COUNT(*) >= 50 ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 2);
        Assert.assertEquals(query.getAggregation().getAttributes().get(TopK.NEW_NAME_FIELD), "count");
        Assert.assertEquals(query.getAggregation().getAttributes().get(TopK.THRESHOLD_FIELD), 50L);
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithComputationAndAlias() {
        build("SELECT abc + 5 AS def, COUNT(*) FROM STREAM() GROUP BY abc + 5 ORDER BY COUNT(*) DESC LIMIT 10");

    }

    @Test
    public void testNotSpecialKMissingLimit() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc", new FieldExpression("abc"))));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 2);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), "COUNT");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "COUNT(*)");

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "COUNT(*)");
        Assert.assertEquals(orderBy.getFields().get(0).getDirection(), OrderBy.Direction.DESC);
    }

    @Test
    public void testQuotedField() {
        build("SELECT \"abc\" FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc");
        Assert.assertEquals(field.getValue(), new FieldExpression("abc"));
    }

    @Test
    public void testFieldExpressionWithIndex() {
        build("SELECT abc[0] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc[0]");
        Assert.assertEquals(field.getValue(), new FieldExpression("abc", 0));
    }

    @Test
    public void testFieldExpressionWithIndexAndSubKey() {
        build("SELECT abc[0].def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc[0].def");
        Assert.assertEquals(field.getValue(), new FieldExpression("abc", 0, "def"));
    }

    @Test
    public void testFieldExpressionWithKey() {
        build("SELECT abc.def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc.def");
        Assert.assertEquals(field.getValue(), new FieldExpression("abc", "def"));
    }

    @Test
    public void testFieldExpressionWithKeyAndSubKey() {
        build("SELECT abc.def.one FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc.def.one");
        Assert.assertEquals(field.getValue(), new FieldExpression("abc", "def", "one"));
    }

    @Test
    public void testQuotedFieldExpressionWithKeyAndSubKey() {
        build("SELECT \"abc\".\"def\".\"one\" FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc.def.one");
        Assert.assertEquals(field.getValue(), new FieldExpression("abc", "def", "one"));
    }

    @Test
    public void testUnaryExpression() {
        build("SELECT SIZEOF(abc) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "SIZEOF(abc)");
        Assert.assertEquals(field.getValue(), new UnaryExpression(new FieldExpression("abc"), Operation.SIZE_OF));
    }

    @Test
    public void testListExpression() {
        build("SELECT [abc, def, one, 5] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "[abc, def, one, 5]");
        Assert.assertEquals(field.getValue(), new ListExpression(Arrays.asList(new FieldExpression("abc"),
                                                                               new FieldExpression("def"),
                                                                               new FieldExpression("one"),
                                                                               new ValueExpression(5))));
    }

    @Test
    public void testNAryExpression() {
        build("SELECT AND(abc, def, one, true, false) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "AND(abc, def, one, true, false)");
        Assert.assertEquals(field.getValue(), new NAryExpression(Arrays.asList(new FieldExpression("abc"),
                                                                               new FieldExpression("def"),
                                                                               new FieldExpression("one"),
                                                                               new ValueExpression(true),
                                                                               new ValueExpression(false)),
                                                                 Operation.AND));
    }

    @Test
    public void testCastExpression() {
        build("SELECT CAST(abc : INTEGER AS DOUBLE) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "CAST(abc : INTEGER AS DOUBLE)");
        Assert.assertEquals(field.getValue(), new CastExpression(new FieldExpression("abc", null, null, null, Type.INTEGER, null), Type.DOUBLE));
    }

    @Test
    public void testIsNull() {
        build("SELECT abc IS NULL FROM STREAM() WHERE abc IS NOT NULL");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc IS NULL");
        Assert.assertEquals(field.getValue(), new UnaryExpression(new FieldExpression("abc"), Operation.IS_NULL));
    }

    @Test
    public void testIsNotNull() {
        build("SELECT abc IS NOT NULL FROM STREAM() WHERE abc IS NOT NULL");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc IS NULL");
        Assert.assertEquals(field.getValue(), new UnaryExpression(new FieldExpression("abc"), Operation.IS_NOT_NULL));
    }

    @Test
    public void testNullValue() {
        build("SELECT NULL FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("NULL", new ValueExpression(null))));
    }
}
