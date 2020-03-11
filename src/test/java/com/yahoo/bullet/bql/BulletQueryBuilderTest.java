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
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Culling;
import com.yahoo.bullet.parsing.Field;
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
import com.yahoo.bullet.typesystem.Schema;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BulletQueryBuilderTest {
    private static final Schema SCHEMA = new Schema(Arrays.asList(new Schema.PlainField("abc", Type.INTEGER),
                                                                  new Schema.PlainField("def", Type.FLOAT),
                                                                  new Schema.PlainField("aaa", Type.STRING_MAP_LIST),
                                                                  new Schema.PlainField("bbb", Type.STRING_MAP_MAP),
                                                                  new Schema.PlainField("ccc", Type.INTEGER_LIST),
                                                                  new Schema.PlainField("ddd", Type.STRING_MAP),
                                                                  new Schema.PlainField("eee", Type.STRING_LIST),
                                                                  new Schema.PlainField("a", Type.LONG),
                                                                  new Schema.PlainField("b", Type.BOOLEAN),
                                                                  new Schema.PlainField("c", Type.STRING)));

    private BulletQueryBuilder builder;
    private Query query;
    private List<BulletError> errors;

    @BeforeClass
    public void setup() {
        builder = new BulletQueryBuilder(new BulletConfig());
        builder.setSchema(SCHEMA);
    }

    private void build(String bql) {
        BQLResult result = builder.buildQuery(bql);
        query = result.getQuery();
        errors = result.getErrors();
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
    public void testWhereNotCastable() {
        build("SELECT * FROM STREAM() WHERE aaa");
        Assert.assertEquals(errors.get(0).getError(), "WHERE clause cannot be casted to BOOLEAN: aaa");
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
        Assert.assertEquals(query.getFilter(), new FieldExpression("abc"));
    }

    @Test
    public void testLimit() {
        build("SELECT * FROM STREAM() LIMIT 10");
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.RAW);
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

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = ".*mismatched input.*")
    public void testRecordDurationNotParsed() {
        build("SELECT * FROM STREAM(2000, TIME, 20, RECORD)");
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
    public void testFieldDoesNotExist() {
        build("SELECT foo FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The field foo does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testSubFieldTypeInvalid() {
        build("SELECT abc[0] FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The subfield abc[0] is invalid since the field abc has type: INTEGER");
        Assert.assertEquals(errors.size(), 1);

        build("SELECT abc[0].def FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The subfield abc[0].def is invalid since the field abc has type: INTEGER");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testRawWithUnnecessaryParentheses() {
        build("SELECT ((abc + 5)) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                new ValueExpression(5),
                                                                                                                Operation.ADD)));
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
    public void testEmptyRawAliasNotAllowed() {
        build("SELECT abc AS \"\" FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Cannot have an empty string as an alias.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testRawAliasesClash() {
        build("SELECT abc, def AS abc, aaa, bbb AS aaa, ccc FROM STREAM()");
        Assert.assertTrue(errors.get(0).getError().equals("The following field names/aliases are shared: [abc, aaa]") ||
                          errors.get(0).getError().equals("The following field names/aliases are shared: [aaa, abc]"));
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testRawAllWithField() {
        build("SELECT *, abc FROM STREAM()");
        Assert.assertNull(query.getProjection());
    }

    @Test
    public void testRawAllWithAlias() {
        build("SELECT *, abc AS def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("def", new FieldExpression("abc")));
        Assert.assertTrue(query.getProjection().isCopy());
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testRawAllWithComputation() {
        build("SELECT *, abc + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                new ValueExpression(5),
                                                                                                                Operation.ADD)));
        Assert.assertTrue(query.getProjection().isCopy());
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testRawAllWithSubField() {
        build("SELECT *, ccc[0] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("ccc[0]", new FieldExpression("ccc", 0)));
        Assert.assertTrue(query.getProjection().isCopy());
        Assert.assertNull(query.getPostAggregations());
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
    public void testRawAllWithOrderByNonPrimitiveNotAllowed() {
        build("SELECT * FROM STREAM() ORDER BY aaa");
        Assert.assertEquals(errors.get(0).getError(), "ORDER BY contains a non-primitive field: aaa");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testRawAllWithOrderByComputation() {
        build("SELECT * FROM STREAM() ORDER BY abc + 5");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                new ValueExpression(5),
                                                                                                                Operation.ADD)));
        Assert.assertTrue(query.getProjection().isCopy());
        Assert.assertEquals(query.getPostAggregations().size(), 2);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc + 5");

        Culling culling = (Culling) query.getPostAggregations().get(1);

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
        build("SELECT abc FROM STREAM() ORDER BY abc + 5 ASC, def DESC");
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
        Assert.assertEquals(field.getType(), Type.STRING_MAP_LIST);
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
        build("SELECT DISTINCT abc FROM STREAM() ORDER BY abc");
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc");
    }

    @Test
    public void testDistinctWithInvalidOrderByField() {
        build("SELECT DISTINCT abc FROM STREAM() ORDER BY def");
        Assert.assertEquals(errors.get(0).getError(), "ORDER BY contains a non-existent field: def");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testDistinctWithOrderByComputation() {
        build("SELECT DISTINCT abc FROM STREAM() ORDER BY abc + 5");
        Assert.assertEquals(query.getPostAggregations().size(), 3);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields().size(), 1);
        Assert.assertEquals(computation.getFields().get(0), new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                      new ValueExpression(5),
                                                                                                      Operation.ADD)));

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(1);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc + 5");

        Culling culling = (Culling) query.getPostAggregations().get(2);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("abc + 5"));
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
    public void testGroupByWithComputationAndOrderByComputation() {
        // This creates a query that has an ORDER BY clause dependent on "abc" (which does not exist after the group by..)
        build("SELECT abc + 5 FROM STREAM() GROUP BY abc + 5 ORDER BY (abc + 5) * 10");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                                   new ValueExpression(5),
                                                                                                                                   Operation.ADD))));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc + 5", "abc + 5"));
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertEquals(query.getPostAggregations().size(), 3);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Field field = new Field("(abc + 5) * 10", new BinaryExpression(new FieldExpression("abc + 5"),
                                                                       new ValueExpression(10),
                                                                       Operation.MUL));

        Assert.assertEquals(computation.getFields(), Collections.singletonList(field));

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(1);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "(abc + 5) * 10");

        Culling culling = (Culling) query.getPostAggregations().get(2);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("(abc + 5) * 10"));
    }

    @Test
    public void testGroupByAggregatesNotAllowed() {
        build("SELECT AVG(abc) FROM STREAM() GROUP BY AVG(abc)");
        Assert.assertEquals(errors.get(0).getError(), "GROUP BY clause cannot contain aggregates.");
        Assert.assertEquals(errors.size(), 1);
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
        Assert.assertEquals(operations.get(0).size(), 3);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "abc");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), GroupOperation.GroupOperationType.AVG);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(abc)");

        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testGroupOpSum() {
        build("SELECT SUM(abc) FROM STREAM()");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertNull(query.getAggregation().getFields());
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 3);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "abc");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), GroupOperation.GroupOperationType.SUM);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "SUM(abc)");

        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testGroupOpNotNumber() {
        build("SELECT AVG(aaa) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The type of the argument in AVG(aaa) must be numeric. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testGroupOpAliasesClash() {
        build("SELECT AVG(abc) AS avg, AVG(def) AS avg FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The following field names/aliases are shared: [avg]");
        Assert.assertEquals(errors.size(), 1);
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
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), GroupOperation.GroupOperationType.AVG);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(abc)");

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Culling culling = (Culling) query.getPostAggregations().get(0);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("abc"));
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
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), GroupOperation.GroupOperationType.AVG);
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
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), GroupOperation.GroupOperationType.AVG);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(abc)");

        Assert.assertEquals(query.getPostAggregations().size(), 2);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("AVG(abc) + 5", new BinaryExpression(new FieldExpression("AVG(abc)"),
                                                                                                                                              new ValueExpression(5),
                                                                                                                                              Operation.ADD))));

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("AVG(abc)"));
    }

    @Test
    public void testGroupOpAsComputationAliasesClash() {
        build("SELECT AVG(abc) + 5 AS sum, AVG(def) + 5 AS sum FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The following field names/aliases are shared: [sum]");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testGroupOpNestingNotAllowed() {
        build("SELECT AVG(SUM(abc)) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Aggregates cannot be nested.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testGroupByHaving() {
        build("SELECT AVG(abc) FROM STREAM() GROUP BY def HAVING AVG(abc) >= 5");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("def", "def"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 3);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "abc");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), GroupOperation.GroupOperationType.AVG);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(abc)");

        Assert.assertEquals(query.getPostAggregations().size(), 2);

        Having having = (Having) query.getPostAggregations().get(0);

        Assert.assertEquals(having.getExpression(), new BinaryExpression(new FieldExpression("AVG(abc)"),
                                                                         new ValueExpression(5),
                                                                         Operation.GREATER_THAN_OR_EQUALS));

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("def"));
    }

    @Test
    public void testHavingNotCastable() {
        build("SELECT AVG(abc) FROM STREAM() GROUP BY aaa HAVING aaa");
        Assert.assertEquals(errors.get(0).getError(), "HAVING clause cannot be casted to BOOLEAN: aaa");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testHavingNoGroupBy() {
        build("SELECT AVG(abc) FROM STREAM() HAVING AVG(abc) >= 5");
        Assert.assertEquals(errors.get(0).getError(), "HAVING clause is only supported with GROUP BY clause.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testNonGroupByHavingNotAllowed() {
        build("SELECT * FROM STREAM() HAVING abc >= 5");
        Assert.assertEquals(errors.get(0).getError(), "HAVING clause is only supported with GROUP BY clause.");
    }

    @Test
    public void testGroupByHavingUsesFieldAlias() {
        build("SELECT abc AS def, AVG(abc) FROM STREAM() GROUP BY abc HAVING abc >= 5");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "def"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 3);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_FIELD), "abc");
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), GroupOperation.GroupOperationType.AVG);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "AVG(abc)");

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Having having = (Having) query.getPostAggregations().get(0);

        Assert.assertEquals(having.getExpression(), new BinaryExpression(new FieldExpression("def"),
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
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), GroupOperation.GroupOperationType.AVG);
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
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), GroupOperation.GroupOperationType.AVG);
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
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), GroupOperation.GroupOperationType.AVG);
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

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("COUNT(DISTINCT abc) + 5", new BinaryExpression(new FieldExpression("COUNT(DISTINCT abc)"),
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

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("COUNT(DISTINCT abc) + 5", new BinaryExpression(new FieldExpression("COUNT(DISTINCT abc)"),
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

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("COUNT(DISTINCT abc) + 5", new BinaryExpression(new FieldExpression("count"),
                                                                                                                                         new ValueExpression(5),
                                                                                                                                         Operation.ADD))));
    }

    @Test
    public void testCountDistinctMultipleNotAllowed() {
        build("SELECT COUNT(DISTINCT abc), COUNT(DISTINCT def) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Cannot have multiple count distincts.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testCountDistinctLimitNotAllowed() {
        build("SELECT COUNT(DISTINCT abc) FROM STREAM() LIMIT 10");
        Assert.assertEquals(errors.get(0).getError(), "LIMIT clause is not supported for queries with count distinct.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testCountDistinctOrderByNotAllowed() {
        build("SELECT COUNT(DISTINCT abc) FROM STREAM() ORDER BY COUNT(DISTINCT abc)");
        Assert.assertEquals(errors.get(0).getError(), "ORDER BY clause is not supported for queries with count distinct.");
        Assert.assertEquals(errors.size(), 1);
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
    public void testDistributionMultipleNotAllowed() {
        build("SELECT FREQ(abc, REGION, 2000, 20000, 500), CUMFREQ(abc, MANUAL, 20000, 2000, 15000, 45000) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Cannot have multiple distributions.");
    }

    @Test
    public void testDistributionAsValueNotAllowed() {
        build("SELECT QUANTILE(abc, LINEAR, 11) + 5 FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Distributions cannot be treated as values.");
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
    public void testTopKMultipleNotAllowed() {
        build("SELECT TOP(10, abc), TOP(10, def) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Cannot have multiple top k.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testTopKAsValueNotAllowed() {
        build("SELECT TOP(10, abc) + 5 FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Top k cannot be treated as a value.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testTopKLimitNotAllowed() {
        build("SELECT TOP(10, abc) FROM STREAM() LIMIT 10");
        Assert.assertEquals(errors.get(0).getError(), "LIMIT clause is not supported for queries with top k.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testTopKOrderByNotAllowed() {
        build("SELECT TOP(10, abc) FROM STREAM() ORDER BY abc");
        Assert.assertEquals(errors.get(0).getError(), "ORDER BY clause is not supported for queries with top k.");
        Assert.assertEquals(errors.size(), 1);
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
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", new FieldExpression("abc")));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("def + 5", new BinaryExpression(new FieldExpression("def"),
                                                                                                                new ValueExpression(5),
                                                                                                                Operation.ADD)));
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
        build("SELECT TOP(10, abc, def + 5), abc + 5, COUNT + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", new FieldExpression("abc")));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("def + 5", new BinaryExpression(new FieldExpression("def"),
                                                                                                                new ValueExpression(5),
                                                                                                                Operation.ADD)));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields().size(), 2);
        Assert.assertEquals(query.getAggregation().getFields().get("abc"), "abc");
        Assert.assertEquals(query.getAggregation().getFields().get("def + 5"), "def + 5");
        Assert.assertNull(query.getAggregation().getAttributes());
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields().size(), 2);
        Assert.assertEquals(computation.getFields().get(0), new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                      new ValueExpression(5),
                                                                                                      Operation.ADD)));
        Assert.assertEquals(computation.getFields().get(1), new Field("COUNT + 5", new BinaryExpression(new FieldExpression("COUNT"),
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

        Assert.assertEquals(computation.getFields().size(), 1);
        Assert.assertEquals(computation.getFields().get(0), new Field("top + 5", new BinaryExpression(new FieldExpression("top"),
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
        Assert.assertEquals(query.getAggregation().getAttributes().get(TopK.THRESHOLD_FIELD), 100);
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
        Assert.assertEquals(query.getAggregation().getAttributes().get(TopK.THRESHOLD_FIELD), 50);
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
        Assert.assertEquals(query.getAggregation().getAttributes().get(TopK.THRESHOLD_FIELD), 50);
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithComputationAndAlias() {
        build("SELECT abc + 5 AS def, COUNT(*) FROM STREAM() GROUP BY abc + 5 ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", new BinaryExpression(new FieldExpression("abc"),
                                                                                                                                   new ValueExpression(5),
                                                                                                                                   Operation.ADD))));
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc + 5", "def"));
        Assert.assertEquals(query.getAggregation().getAttributes(), Collections.singletonMap(TopK.NEW_NAME_FIELD, "COUNT(*)"));
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testNotSpecialKMissingLimit() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC");
        Assert.assertNull(query.getProjection());
        Assert.assertEquals(query.getAggregation().getType(), Aggregation.Type.GROUP);
        Assert.assertEquals(query.getAggregation().getFields(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(query.getAggregation().getAttributes().size(), 1);

        List<Map<String, Object>> operations = (List<Map<String, Object>>) query.getAggregation().getAttributes().get(GroupOperation.OPERATIONS);

        Assert.assertEquals(operations.size(), 1);
        Assert.assertEquals(operations.get(0).size(), 2);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_TYPE), GroupOperation.GroupOperationType.COUNT);
        Assert.assertEquals(operations.get(0).get(GroupOperation.OPERATION_NEW_NAME), "COUNT(*)");

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "COUNT(*)");
        Assert.assertEquals(orderBy.getFields().get(0).getDirection(), OrderBy.Direction.DESC);
    }

    @Test
    public void testNotSpecialKNotCount() {
        build("SELECT abc, AVG(abc) FROM STREAM() GROUP BY abc ORDER BY AVG(abc) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
    }

    @Test
    public void testNotSpecialKFieldsNotSelected() {
        build("SELECT COUNT(*) FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
    }

    @Test
    public void testNotSpecialKCountNotSelected() {
        build("SELECT abc FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
    }

    @Test
    public void testNotSpecialKNotCountOrderBy() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc ORDER BY abc DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
    }

    @Test
    public void testNotSpecialKNotOrderByDesc() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc ORDER BY COUNT(*) ASC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);
    }

    @Test
    public void testNotSpecialKBadHaving() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING COUNT(*) ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);

        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING abc >= 5 ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);

        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING COUNT(*) > 5 ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);

        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING COUNT(*) >= abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), Aggregation.Type.TOP_K);

        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING COUNT(*) >= '5' ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(errors.get(0).getError(), "The right operand in COUNT(*) >= '5' must be numeric. Type given: STRING");
        Assert.assertEquals(errors.size(), 1);
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
        build("SELECT aaa[0] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "aaa[0]");
        Assert.assertEquals(field.getValue(), new FieldExpression("aaa", 0));
        Assert.assertEquals(field.getValue().getType(), Type.STRING_MAP);
    }

    @Test
    public void testFieldExpressionWithIndexAndSubKey() {
        build("SELECT aaa[0].def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "aaa[0].def");
        Assert.assertEquals(field.getValue(), new FieldExpression("aaa", 0, "def"));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testFieldExpressionWithKey() {
        build("SELECT bbb.def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb.def");
        Assert.assertEquals(field.getValue(), new FieldExpression("bbb", "def"));
        Assert.assertEquals(field.getValue().getType(), Type.STRING_MAP);
    }

    @Test
    public void testFieldExpressionWithKeyAndSubKey() {
        build("SELECT bbb.def.one FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb.def.one");
        Assert.assertEquals(field.getValue(), new FieldExpression("bbb", "def", "one"));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testQuotedFieldExpressionWithKeyAndSubKey() {
        build("SELECT \"bbb\".\"def\".\"one\" FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb.def.one");
        Assert.assertEquals(field.getValue(), new FieldExpression("bbb", "def", "one"));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testSignedNumbers() {
        build("SELECT 5 AS a, -5 AS b, 5L AS c, -5L AS d, 5.0 AS e, -5.0 AS f, 5.0f AS g, -5.0f AS h FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 8);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("a", new ValueExpression(5)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("b", new ValueExpression(-5)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("c", new ValueExpression(5L)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("d", new ValueExpression(-5L)));
        Assert.assertEquals(query.getProjection().getFields().get(4), new Field("e", new ValueExpression(5.0)));
        Assert.assertEquals(query.getProjection().getFields().get(5), new Field("f", new ValueExpression(-5.0)));
        Assert.assertEquals(query.getProjection().getFields().get(6), new Field("g", new ValueExpression(5.0f)));
        Assert.assertEquals(query.getProjection().getFields().get(7), new Field("h", new ValueExpression(-5.0f)));
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testBoolean() {
        build("SELECT true, false FROM STREAM()");
        build("SELECT true, false FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("true", new ValueExpression(true)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("false", new ValueExpression(false)));
    }

    @Test
    public void testBinaryOperations() {
        build("SELECT a + 5, a - 5, a * 5, a / 5, a = 5, a != 5, a > 5, a < 5, a >= 5, a <= 5, " +
              "RLIKE(c, 'abc'), SIZEIS(c, 5), CONTAINSKEY(bbb, 'abc'), CONTAINSVALUE(aaa, 'abc'), 'abc' IN aaa, FILTER(aaa, [true, false]), " +
              "b AND true, b OR false, b XOR true FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 19);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("a + 5", new BinaryExpression(new FieldExpression("a"),
                                                                                                              new ValueExpression(5),
                                                                                                              Operation.ADD)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("a - 5", new BinaryExpression(new FieldExpression("a"),
                                                                                                              new ValueExpression(5),
                                                                                                              Operation.SUB)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("a * 5", new BinaryExpression(new FieldExpression("a"),
                                                                                                              new ValueExpression(5),
                                                                                                              Operation.MUL)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("a / 5", new BinaryExpression(new FieldExpression("a"),
                                                                                                              new ValueExpression(5),
                                                                                                              Operation.DIV)));
        Assert.assertEquals(query.getProjection().getFields().get(4), new Field("a = 5", new BinaryExpression(new FieldExpression("a"),
                                                                                                              new ValueExpression(5),
                                                                                                              Operation.EQUALS)));
        Assert.assertEquals(query.getProjection().getFields().get(5), new Field("a != 5", new BinaryExpression(new FieldExpression("a"),
                                                                                                               new ValueExpression(5),
                                                                                                               Operation.NOT_EQUALS)));
        Assert.assertEquals(query.getProjection().getFields().get(6), new Field("a > 5", new BinaryExpression(new FieldExpression("a"),
                                                                                                              new ValueExpression(5),
                                                                                                              Operation.GREATER_THAN)));
        Assert.assertEquals(query.getProjection().getFields().get(7), new Field("a < 5", new BinaryExpression(new FieldExpression("a"),
                                                                                                              new ValueExpression(5),
                                                                                                              Operation.LESS_THAN)));
        Assert.assertEquals(query.getProjection().getFields().get(8), new Field("a >= 5", new BinaryExpression(new FieldExpression("a"),
                                                                                                               new ValueExpression(5),
                                                                                                               Operation.GREATER_THAN_OR_EQUALS)));
        Assert.assertEquals(query.getProjection().getFields().get(9), new Field("a <= 5", new BinaryExpression(new FieldExpression("a"),
                                                                                                               new ValueExpression(5),
                                                                                                               Operation.LESS_THAN_OR_EQUALS)));
        Assert.assertEquals(query.getProjection().getFields().get(10), new Field("RLIKE(c, 'abc')", new BinaryExpression(new FieldExpression("c"),
                                                                                                                         new ValueExpression("abc"),
                                                                                                                         Operation.REGEX_LIKE)));
        Assert.assertEquals(query.getProjection().getFields().get(11), new Field("SIZEIS(c, 5)", new BinaryExpression(new FieldExpression("c"),
                                                                                                                      new ValueExpression(5),
                                                                                                                      Operation.SIZE_IS)));
        Assert.assertEquals(query.getProjection().getFields().get(12), new Field("CONTAINSKEY(bbb, 'abc')", new BinaryExpression(new FieldExpression("bbb"),
                                                                                                                                 new ValueExpression("abc"),
                                                                                                                                 Operation.CONTAINS_KEY)));
        Assert.assertEquals(query.getProjection().getFields().get(13), new Field("CONTAINSVALUE(aaa, 'abc')", new BinaryExpression(new FieldExpression("aaa"),
                                                                                                                                   new ValueExpression("abc"),
                                                                                                                                   Operation.CONTAINS_VALUE)));
        Assert.assertEquals(query.getProjection().getFields().get(14), new Field("'abc' IN aaa", new BinaryExpression(new ValueExpression("abc"),
                                                                                                                      new FieldExpression("aaa"),
                                                                                                                      Operation.IN)));
        Assert.assertEquals(query.getProjection().getFields().get(15), new Field("FILTER(aaa, [true, false])", new BinaryExpression(new FieldExpression("aaa"),
                                                                                                                                    new ListExpression(Arrays.asList(new ValueExpression(true), new ValueExpression(false))),
                                                                                                                                    Operation.FILTER)));
        Assert.assertEquals(query.getProjection().getFields().get(16), new Field("b AND true", new BinaryExpression(new FieldExpression("b"),
                                                                                                                    new ValueExpression(true),
                                                                                                                    Operation.AND)));
        Assert.assertEquals(query.getProjection().getFields().get(17), new Field("b OR false", new BinaryExpression(new FieldExpression("b"),
                                                                                                                    new ValueExpression(false),
                                                                                                                    Operation.OR)));
        Assert.assertEquals(query.getProjection().getFields().get(18), new Field("b XOR true", new BinaryExpression(new FieldExpression("b"),
                                                                                                                    new ValueExpression(true),
                                                                                                                    Operation.XOR)));
    }

    @Test
    public void testTypeCheckNumericOperation() {
        build("SELECT 'foo' + 'bar', 'foo' + 0, 0 + 'foo' FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The left and right operands in 'foo' + 'bar' must be numbers. Types given: STRING, STRING");
        Assert.assertEquals(errors.get(1).getError(), "The left and right operands in 'foo' + 0 must be numbers. Types given: STRING, INTEGER");
        Assert.assertEquals(errors.get(2).getError(), "The left and right operands in 0 + 'foo' must be numbers. Types given: INTEGER, STRING");
        Assert.assertEquals(errors.size(), 3);
    }

    @Test
    public void testTypeCheckComparison() {
        build("SELECT 'foo' > 'bar', 'foo' = 0, 0 != 'foo', 'foo' = 'bar' FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The left operand in 'foo' > 'bar' must be numeric. Type given: STRING");
        Assert.assertEquals(errors.get(1).getError(), "The right operand in 'foo' > 'bar' must be numeric. Type given: STRING");
        Assert.assertEquals(errors.get(2).getError(), "The left and right operands in 'foo' = 0 must be comparable or have the same type. Types given: STRING, INTEGER");
        Assert.assertEquals(errors.get(3).getError(), "The left and right operands in 0 != 'foo' must be comparable or have the same type. Types given: INTEGER, STRING");
        Assert.assertEquals(errors.size(), 4);
    }

    @Test
    public void testTypeCheckComparisonModifier() {
        build("SELECT 5 > ANY aaa, 5 > ANY ccc, 5 > ALL eee, 5 = ANY ccc, 5 = ALL 'foo', 5 = ANY aaa, 'foo' = ALL eee FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The right operand in 5 > ANY aaa must be some numeric LIST. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.get(1).getError(), "The right operand in 5 > ALL eee must be some numeric LIST. Type given: STRING_LIST");
        Assert.assertEquals(errors.get(2).getError(), "The right operand in 5 = ALL 'foo' must be some LIST. Type given: STRING");
        Assert.assertEquals(errors.get(3).getError(), "The type of the left operand and the subtype of the right operand in 5 = ANY aaa must be comparable or the same. Types given: INTEGER, STRING_MAP_LIST");
        Assert.assertEquals(errors.size(), 4);
    }

    @Test
    public void testTypeCheckRegexLike() {
        build("SELECT RLIKE('foo', 0), RLIKE(0, 'foo') FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The types of the arguments in RLIKE('foo', 0) must be STRING. Types given: STRING, INTEGER");
        Assert.assertEquals(errors.get(1).getError(), "The types of the arguments in RLIKE(0, 'foo') must be STRING. Types given: INTEGER, STRING");
        Assert.assertEquals(errors.size(), 2);
    }

    @Test
    public void testTypeCheckSizeIs() {
        build("SELECT SIZEIS(abc, 5), SIZEIS(aaa, 'foo'), SIZEIS('foo', 'foo') FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The type of the first argument in SIZEIS(abc, 5) must be some LIST, MAP, or STRING. Type given: INTEGER");
        Assert.assertEquals(errors.get(1).getError(), "The type of the second argument in SIZEIS(aaa, 'foo') must be numeric. Type given: STRING");
        Assert.assertEquals(errors.get(2).getError(), "The type of the second argument in SIZEIS('foo', 'foo') must be numeric. Type given: STRING");
        Assert.assertEquals(errors.size(), 3);
    }

    @Test
    public void testTypeCheckContainsKey() {
        build("SELECT CONTAINSKEY('foo', 5), CONTAINSKEY(aaa, 'foo'), CONTAINSKEY(bbb, 'foo') FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The type of the first argument in CONTAINSKEY('foo', 5) must be some MAP or MAP_LIST. Type given: STRING");
        Assert.assertEquals(errors.get(1).getError(), "The type of the second argument in CONTAINSKEY('foo', 5) must be STRING. Type given: INTEGER");
        Assert.assertEquals(errors.size(), 2);
    }

    @Test
    public void testTypeCheckContainsValue() {
        build("SELECT CONTAINSVALUE('foo', aaa), CONTAINSVALUE(aaa, 5), CONTAINSVALUE(ddd, 5), CONTAINSVALUE(ddd, c) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The type of the first argument in CONTAINSVALUE('foo', aaa) must be some LIST or MAP. Type given: STRING");
        Assert.assertEquals(errors.get(1).getError(), "The type of the second argument in CONTAINSVALUE('foo', aaa) must be primitive. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.get(2).getError(), "The primitive type of the first argument and the type of the second argument in CONTAINSVALUE(aaa, 5) must match. Types given: STRING_MAP_LIST, INTEGER");
        Assert.assertEquals(errors.get(3).getError(), "The primitive type of the first argument and the type of the second argument in CONTAINSVALUE(ddd, 5) must match. Types given: STRING_MAP, INTEGER");
        Assert.assertEquals(errors.size(), 4);
    }

    @Test
    public void testTypeCheckIn() {
        build("SELECT aaa IN 'foo', 5 IN aaa, 5 IN ddd, c IN ddd FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The type of the left operand in aaa IN 'foo' must be primitive. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.get(1).getError(), "The type of the right operand in aaa IN 'foo' must be some LIST or MAP. Type given: STRING");
        Assert.assertEquals(errors.get(2).getError(), "The type of the left operand and the primitive type of the right operand in 5 IN aaa must match. Types given: INTEGER, STRING_MAP_LIST");
        Assert.assertEquals(errors.get(3).getError(), "The type of the left operand and the primitive type of the right operand in 5 IN ddd must match. Types given: INTEGER, STRING_MAP");
        Assert.assertEquals(errors.size(), 4);
    }

    @Test
    public void testTypeCheckBooleanComparison() {
        build("SELECT 5 AND true, false OR 5, 'foo' XOR 5 FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The types of the arguments in 5 AND true must be BOOLEAN. Types given: INTEGER, BOOLEAN");
        Assert.assertEquals(errors.get(1).getError(), "The types of the arguments in false OR 5 must be BOOLEAN. Types given: BOOLEAN, INTEGER");
        Assert.assertEquals(errors.get(2).getError(), "The types of the arguments in 'foo' XOR 5 must be BOOLEAN. Types given: STRING, INTEGER");
        Assert.assertEquals(errors.size(), 3);
    }

    @Test
    public void testTypeCheckFilter() {
        build("SELECT FILTER('foo', 5) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The type of the first argument in FILTER('foo', 5) must be some LIST. Type given: STRING");
        Assert.assertEquals(errors.get(1).getError(), "The type of the second argument in FILTER('foo', 5) must be BOOLEAN_LIST. Type given: INTEGER");
        Assert.assertEquals(errors.size(), 2);
    }

    @Test
    public void testTypes() {
        build("SELECT a : STRING, b : LIST[STRING], c : MAP[STRING], d : LIST[MAP[STRING]], e : MAP[MAP[STRING]] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 5);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("a", new FieldExpression("a", null, null, null, Type.STRING)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("b", new FieldExpression("b", null, null, null, Type.STRING_LIST)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("c", new FieldExpression("c", null, null, null, Type.STRING_MAP)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("d", new FieldExpression("d", null, null, null, Type.STRING_MAP_LIST)));
        Assert.assertEquals(query.getProjection().getFields().get(4), new Field("e", new FieldExpression("e", null, null, null, Type.STRING_MAP_MAP)));
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testUnaryExpressionSizeOfCollection() {
        build("SELECT SIZEOF(aaa) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "SIZEOF(aaa)");
        Assert.assertEquals(field.getValue(), new UnaryExpression(new FieldExpression("aaa"), Operation.SIZE_OF));
    }

    @Test
    public void testUnaryExpressionSizeOfString() {
        build("SELECT SIZEOF(c) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "SIZEOF(c)");
        Assert.assertEquals(field.getValue(), new UnaryExpression(new FieldExpression("c"), Operation.SIZE_OF));
    }

    @Test
    public void testUnaryExpressionSizeOfInvalid() {
        build("SELECT SIZEOF(5) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The type of the argument in SIZEOF(5) must be some LIST, MAP, or STRING. Type given: INTEGER");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testUnaryExpressionNotNumber() {
        build("SELECT NOT abc FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "NOT abc");
        Assert.assertEquals(field.getValue(), new UnaryExpression(new FieldExpression("abc"), Operation.NOT));
    }

    @Test
    public void testUnaryExpressionNotBoolean() {
        build("SELECT NOT b FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "NOT b");
        Assert.assertEquals(field.getValue(), new UnaryExpression(new FieldExpression("b"), Operation.NOT));
    }

    @Test
    public void testUnaryExpressionNotInvalid() {
        build("SELECT NOT 'foo' FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The type of the argument in NOT 'foo' must be numeric or BOOLEAN. Type given: STRING");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testListExpression() {
        build("SELECT [abc, 5, 10] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "[abc, 5, 10]");
        Assert.assertEquals(field.getValue(), new ListExpression(Arrays.asList(new FieldExpression("abc"),
                                                                               new ValueExpression(5),
                                                                               new ValueExpression(10))));
        Assert.assertEquals(field.getValue().getType(), Type.INTEGER_LIST);
    }

    @Test
    public void testListExpressionSubMap() {
        build("SELECT [ddd, ddd, ddd] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "[ddd, ddd, ddd]");
        Assert.assertEquals(field.getValue(), new ListExpression(Arrays.asList(new FieldExpression("ddd"),
                                                                               new FieldExpression("ddd"),
                                                                               new FieldExpression("ddd"))));
        Assert.assertEquals(field.getValue().getType(), Type.STRING_MAP_LIST);
    }

    @Test
    public void testEmptyListNotAllowed() {
        build("SELECT [] FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Empty lists are currently not supported.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testListExpressionTypeCheckMultiple() {
        build("SELECT [5, 'foo'] FROM STREAM()");
        Assert.assertTrue(errors.get(0).getError().equals("The list [5, 'foo'] consists of objects of multiple types: [INTEGER, STRING]") ||
                          errors.get(0).getError().equals("The list [5, 'foo'] consists of objects of multiple types: [STRING, INTEGER]"));
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testListExpressionTypeCheckSubType() {
        build("SELECT [[5], [10]] FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The list [[5], [10]] must consist of objects of a single primitive or primitive map type. Subtype given: INTEGER_LIST");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testNAryExpression() {
        build("SELECT IF(b, 5, 10) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "IF(b, 5, 10)");
        Assert.assertEquals(field.getValue(), new NAryExpression(Arrays.asList(new FieldExpression("b"),
                                                                               new ValueExpression(5),
                                                                               new ValueExpression(10)),
                                                                 Operation.IF));
    }

    @Test
    public void testNAryExpressionBadArguments() {
        build("SELECT IF(c, 5, 10.0) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The type of the first argument in IF(c, 5, 10.0) must be BOOLEAN. Type given: STRING");
        Assert.assertEquals(errors.get(1).getError(), "The types of the second and third arguments in IF(c, 5, 10.0) must match. Types given: INTEGER, DOUBLE");
    }

    @Test
    public void testCastExpression() {
        build("SELECT CAST(abc : INTEGER AS DOUBLE) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "CAST(abc AS DOUBLE)");
        Assert.assertEquals(field.getValue(), new CastExpression(new FieldExpression("abc", null, null, null, Type.INTEGER), Type.DOUBLE));
    }

    @Test
    public void testCastExpressionInvalid() {
        build("SELECT CAST(aaa AS INTEGER) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Cannot cast aaa from STRING_MAP_LIST to INTEGER.");
        Assert.assertEquals(errors.size(), 1);
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

        Assert.assertEquals(field.getName(), "abc IS NOT NULL");
        Assert.assertEquals(field.getValue(), new UnaryExpression(new FieldExpression("abc"), Operation.IS_NOT_NULL));
    }

    @Test
    public void testNullValue() {
        build("SELECT NULL FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("NULL", new ValueExpression(null))));
    }

    @Test
    public void testTooManyQueryTypes() {
        build("SELECT * FROM STREAM() GROUP BY abc");
        Assert.assertTrue(errors.get(0).getError().startsWith("Query does not match exactly one query type: "));
        Assert.assertEquals(errors.size(), 1);
    }

    // Tests that cover any instance of unknowns i.e. verify that type-checking errors propagate but don't create more error messages

    @Test
    public void testFieldUnknown() {
        // coverage
        build("SELECT AVG(foo) AS bar FROM STREAM() ORDER BY bar + 5");
        Assert.assertEquals(errors.get(0).getError(), "The field foo does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testCountDistinctUnknown() {
        // coverage
        build("SELECT COUNT(DISTINCT foo) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The field foo does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testTopKUnknown() {
        // coverage
        build("SELECT TOP(10, foo) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The field foo does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testDistributionUnknown() {
        build("SELECT QUANTILE(foo, LINEAR, 11) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The field foo does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testUnknowns() {
        // coverage
        build("SELECT [(SIZEIS(CAST(IF(foo IS NOT NULL, 5, 10) AS STRING), 10)) + 5], bar + foo, 5 + car FROM STREAM() WHERE foo");
        Assert.assertEquals(errors.get(0).getError(), "The field foo does not exist in the schema.");
        Assert.assertEquals(errors.get(1).getError(), "The field bar does not exist in the schema.");
        Assert.assertEquals(errors.get(2).getError(), "The field car does not exist in the schema.");
        Assert.assertEquals(errors.size(), 3);
    }
}
