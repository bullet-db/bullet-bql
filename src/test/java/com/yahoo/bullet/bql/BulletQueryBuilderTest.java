/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.bql.extractor.AggregationExtractor;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.aggregations.CountDistinct;
import com.yahoo.bullet.query.aggregations.DistributionType;
import com.yahoo.bullet.query.aggregations.GroupAll;
import com.yahoo.bullet.query.aggregations.GroupBy;
import com.yahoo.bullet.query.aggregations.LinearDistribution;
import com.yahoo.bullet.query.aggregations.ManualDistribution;
import com.yahoo.bullet.query.aggregations.RegionDistribution;
import com.yahoo.bullet.query.aggregations.TopK;
import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.query.postaggregations.Having;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.query.postaggregations.PostAggregationType;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.cast;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.list;
import static com.yahoo.bullet.bql.util.QueryUtil.nary;
import static com.yahoo.bullet.bql.util.QueryUtil.unary;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class BulletQueryBuilderTest {
    private BulletQueryBuilder builder;
    private Query query;
    private List<BulletError> errors;
    private Integer defaultSize;
    private Long defaultDuration;

    @BeforeClass
    public void setup() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RECORD_SCHEMA_FILE_NAME, "test_schema.json");
        config.validate();
        builder = new BulletQueryBuilder(config);
        defaultSize = config.getAs(BulletConfig.AGGREGATION_DEFAULT_SIZE, Integer.class);
        defaultDuration = config.getAs(BulletConfig.QUERY_DEFAULT_DURATION, Long.class);
    }

    private void build(String bql) {
        BQLResult result = builder.buildQuery(bql);
        query = result.getQuery();
        errors = result.getErrors();
    }

    @Test
    public void testRawAll() {
        build("SELECT * FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertNull(query.getFilter());
        Assert.assertEquals(query.getAggregation().getSize(), defaultSize);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertEquals(query.getDuration(), defaultDuration);
    }

    @Test
    public void testWhere() {
        build("SELECT * FROM STREAM() WHERE abc");
        Assert.assertEquals(query.getFilter(), field("abc", Type.INTEGER));
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
        Assert.assertEquals(query.getFilter(), field("abc", Type.INTEGER));
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
    public void testWindowEveryAll() {
        build("SELECT COUNT(*) FROM STREAM() WINDOWING EVERY(5000, TIME, ALL)");
        Assert.assertEquals(query.getWindow().getEmitEvery(), (Integer) 5000);
        Assert.assertEquals(query.getWindow().getEmitType(), Window.Unit.TIME);
        Assert.assertEquals(query.getWindow().getIncludeType(), Window.Unit.ALL);
    }

    @Test
    public void testWindowEveryFirst() {
        build("SELECT * FROM STREAM() WINDOWING EVERY(1, RECORD, FIRST, 1, RECORD)");
        Assert.assertEquals(query.getWindow().getEmitEvery(), (Integer) 1);
        Assert.assertEquals(query.getWindow().getEmitType(), Window.Unit.RECORD);
        Assert.assertEquals(query.getWindow().getIncludeFirst(), (Integer) 1);
        Assert.assertEquals(query.getWindow().getIncludeType(), Window.Unit.RECORD);
    }

    @Test
    public void testWindowTumbling() {
        build("SELECT * FROM STREAM() WINDOWING TUMBLING(5000, TIME)");
        Assert.assertEquals(query.getWindow().getEmitEvery(), (Integer) 5000);
        Assert.assertEquals(query.getWindow().getEmitType(), Window.Unit.TIME);
        Assert.assertNull(query.getWindow().getIncludeType());
    }

    @Test
    public void testRaw() {
        build("SELECT abc FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
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
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                  value(5),
                                                                                                  Operation.ADD,
                                                                                                  Type.INTEGER)));
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
    }

    @Test
    public void testRawAlias() {
        build("SELECT abc AS def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("def", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
    }

    @Test
    public void testQuotedRawAlias() {
        build("SELECT \"abc\" AS \"def\" FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("def", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
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
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
    }

    @Test
    public void testRawAllWithAlias() {
        build("SELECT *, abc AS def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("def", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.COPY);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testRawAllWithComputation() {
        build("SELECT *, abc + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                  value(5),
                                                                                                  Operation.ADD,
                                                                                                  Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.COPY);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testRawAllWithSubField() {
        build("SELECT *, ccc[0] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("ccc[0]", field("ccc", 0, Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.COPY);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testRawAllWithOrderByField() {
        build("SELECT * FROM STREAM() ORDER BY abc");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);

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
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                  value(5),
                                                                                                  Operation.ADD,
                                                                                                  Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.COPY);
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
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc");
    }

    @Test
    public void testRawWithOrderByDifferentField() {
        build("SELECT abc FROM STREAM() ORDER BY def");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("def", field("def", Type.FLOAT)));
        Assert.assertEquals(query.getPostAggregations().size(), 2);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);
        Assert.assertEquals(query.getPostAggregations().get(1).getType(), PostAggregationType.CULLING);

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
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                  value(5),
                                                                                                  Operation.ADD,
                                                                                                  Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("def", field("def", Type.FLOAT)));
        Assert.assertEquals(query.getPostAggregations().size(), 2);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);
        Assert.assertEquals(query.getPostAggregations().get(1).getType(), PostAggregationType.CULLING);

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
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("def", field("abc", Type.INTEGER))));
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "def");
    }

    @Test
    public void testRawAliasWithOrderByAlias() {
        build("SELECT abc AS def FROM STREAM() ORDER BY def");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("def", field("abc", Type.INTEGER))));
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "def");
    }

    @Test
    public void testRawAliasWithOrderByPrioritizeSimpleAlias1() {
        build("SELECT abc AS def, def AS abc FROM STREAM() ORDER BY abc");
        Assert.assertEquals(query.getProjection().getFields(), Arrays.asList(new Field("def", field("abc", Type.INTEGER)),
                                                                             new Field("abc", field("def", Type.FLOAT))));
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc");
    }

    @Test
    public void testRawAliasWithOrderByPrioritizeSimpleAlias2() {
        build("SELECT abc AS def, def AS abc FROM STREAM() ORDER BY def");
        Assert.assertEquals(query.getProjection().getFields(), Arrays.asList(new Field("def", field("abc", Type.INTEGER)),
                                                                             new Field("abc", field("def", Type.FLOAT))));
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "def");
    }

    @Test
    public void testRawAliasWithOrderByNoNestedAlias() {
        build("SELECT abc AS def, def AS abc FROM STREAM() ORDER BY abc + 5");
        Assert.assertEquals(query.getProjection().getFields(), Arrays.asList(new Field("def", field("abc", Type.INTEGER)),
                                                                             new Field("abc", field("def", Type.FLOAT)),
                                                                             new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                         value(5),
                                                                                                         Operation.ADD,
                                                                                                         Type.INTEGER))));
        Assert.assertEquals(query.getPostAggregations().size(), 2);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);
        Assert.assertEquals(query.getPostAggregations().get(1).getType(), PostAggregationType.CULLING);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc + 5");

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("abc + 5"));
    }

    @Test
    public void testSingleDistinct() {
        build("SELECT DISTINCT abc FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 1);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "abc");
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testMultipleDistinct() {
        build("SELECT DISTINCT abc, def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc"));
        Assert.assertTrue(aggregation.getFields().contains("def"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "abc");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def"), "def");
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testMultipleDistinctNonPrimitive() {
        build("SELECT DISTINCT aaa, bbb FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The SELECT DISTINCT field aaa is non-primitive. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.get(1).getError(), "The SELECT DISTINCT field bbb is non-primitive. Type given: STRING_MAP_MAP");
        Assert.assertEquals(errors.size(), 2);
    }

    @Test
    public void testDistinctWithAlias() {
        build("SELECT DISTINCT abc AS one, def AS two FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc"));
        Assert.assertTrue(aggregation.getFields().contains("def"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "one");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def"), "two");
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testDistinctWithComputation() {
        build("SELECT DISTINCT abc + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                  value(5),
                                                                                                  Operation.ADD,
                                                                                                  Type.INTEGER)));

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 1);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc + 5"), "abc + 5");
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(aggregation.getSize(), defaultSize);
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
        Assert.assertEquals(computation.getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                        value(5),
                                                                                        Operation.ADD,
                                                                                        Type.INTEGER)));

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(1);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc + 5");

        Culling culling = (Culling) query.getPostAggregations().get(2);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("abc + 5"));
    }

    @Test
    public void testGroupBy() {
        build("SELECT abc FROM STREAM() GROUP BY abc");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertTrue(aggregation.getOperations().isEmpty());
    }

    @Test
    public void testGroupByNonPrimitive() {
        build("SELECT aaa, bbb FROM STREAM() GROUP BY aaa, bbb");
        Assert.assertEquals(errors.get(0).getError(), "The GROUP BY field aaa is non-primitive. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.get(1).getError(), "The GROUP BY field bbb is non-primitive. Type given: STRING_MAP_MAP");
        Assert.assertEquals(errors.size(), 2);
    }

    @Test
    public void testGroupByWithAlias() {
        build("SELECT abc AS def FROM STREAM() GROUP BY abc");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "def"));
        Assert.assertTrue(aggregation.getOperations().isEmpty());
    }

    @Test
    public void testGroupByComputation() {
        build("SELECT abc + 5 FROM STREAM() GROUP BY abc + 5");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc + 5", "abc + 5"));
        Assert.assertTrue(aggregation.getOperations().isEmpty());
    }

    @Test
    public void testGroupByComputationWithAlias() {
        build("SELECT abc + 5 AS def FROM STREAM() GROUP BY abc + 5");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc + 5", "def"));
        Assert.assertTrue(aggregation.getOperations().isEmpty());
    }

    @Test
    public void testGroupByWithComputationAndOrderBy() {
        build("SELECT abc + 5 FROM STREAM() GROUP BY abc + 5 ORDER BY abc + 5");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc + 5", "abc + 5"));
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "abc + 5");
    }

    @Test
    public void testGroupByWithComputationAliasAndOrderBy() {
        build("SELECT abc + 5 AS def FROM STREAM() GROUP BY abc + 5 ORDER BY def");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc + 5", "def"));
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "def");
    }

    @Test
    public void testGroupByWithComputationAndOrderByComputation() {
        // This creates a query that has an ORDER BY clause dependent on "abc" (which does not exist after the group by..)
        build("SELECT abc + 5 FROM STREAM() GROUP BY abc + 5 ORDER BY (abc + 5) * 10");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc + 5", "abc + 5"));
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(query.getPostAggregations().size(), 3);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Field field = new Field("(abc + 5) * 10", binary(field("abc + 5", Type.INTEGER),
                                                         value(10),
                                                         Operation.MUL,
                                                         Type.INTEGER));

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
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupAll aggregation = (GroupAll) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.emptyList());
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.AVG,
                                                                                                  "abc",
                                                                                                  "AVG(abc)")));
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testGroupOpSum() {
        build("SELECT SUM(abc) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupAll aggregation = (GroupAll) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.emptyList());
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.SUM,
                                                                                                  "abc",
                                                                                                  "SUM(abc)")));
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
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.AVG,
                                                                                                  "abc",
                                                                                                  "AVG(abc)")));

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Culling culling = (Culling) query.getPostAggregations().get(0);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("abc"));
    }

    @Test
    public void testGroupOpWithComputation() {
        build("SELECT AVG(abc + 5) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        GroupAll aggregation = (GroupAll) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.emptyList());
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.AVG,
                                                                                                  "abc + 5",
                                                                                                  "AVG(abc + 5)")));

        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testGroupOpAsComputation() {
        build("SELECT AVG(abc) + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupAll aggregation = (GroupAll) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.emptyList());
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.AVG,
                                                                                                  "abc",
                                                                                                  "AVG(abc)")));

        Assert.assertEquals(query.getPostAggregations().size(), 2);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("AVG(abc) + 5", binary(field("AVG(abc)", Type.DOUBLE),
                                                                                                                value(5),
                                                                                                                Operation.ADD,
                                                                                                                Type.DOUBLE))));

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
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("def"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("def", "def"));
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.AVG,
                                                                                                  "abc",
                                                                                                  "AVG(abc)")));

        Assert.assertEquals(query.getPostAggregations().size(), 2);

        Having having = (Having) query.getPostAggregations().get(0);

        Assert.assertEquals(having.getExpression(), binary(field("AVG(abc)", Type.DOUBLE),
                                                           value(5),
                                                           Operation.GREATER_THAN_OR_EQUALS,
                                                           Type.BOOLEAN));

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("def"));
    }

    @Test
    public void testHavingNotCastable() {
        build("SELECT AVG(abc) FROM STREAM() GROUP BY abc HAVING [abc]");
        Assert.assertEquals(errors.get(0).getError(), "HAVING clause cannot be casted to BOOLEAN: [abc]");
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
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "def"));
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.AVG,
                                                                                                  "abc",
                                                                                                  "AVG(abc)")));

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Having having = (Having) query.getPostAggregations().get(0);

        Assert.assertEquals(having.getExpression(), binary(field("def", Type.INTEGER),
                                                           value(5),
                                                           Operation.GREATER_THAN_OR_EQUALS,
                                                           Type.BOOLEAN));
    }

    @Test
    public void testGroupByHavingUsesAggregateAlias() {
        build("SELECT abc, AVG(abc) AS avg FROM STREAM() GROUP BY abc HAVING AVG(abc) >= 5");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.AVG,
                                                                                                  "abc",
                                                                                                  "AVG(abc)")));

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Having having = (Having) query.getPostAggregations().get(0);

        Assert.assertEquals(having.getExpression(), binary(field("avg", Type.DOUBLE),
                                                           value(5),
                                                           Operation.GREATER_THAN_OR_EQUALS,
                                                           Type.BOOLEAN));
    }

    @Test
    public void testGroupByHavingUnselectedAggregate() {
        build("SELECT abc FROM STREAM() GROUP BY abc HAVING AVG(def) >= 5");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.AVG,
                                                                                                  "def",
                                                                                                  "AVG(def)")));

        Assert.assertEquals(query.getPostAggregations().size(), 2);

        Having having = (Having) query.getPostAggregations().get(0);

        Assert.assertEquals(having.getExpression(), binary(field("AVG(def)", Type.DOUBLE),
                                                           value(5),
                                                           Operation.GREATER_THAN_OR_EQUALS,
                                                           Type.BOOLEAN));

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("AVG(def)"));
    }

    @Test
    public void testGroupByOrderByUnselectedAggregate() {
        build("SELECT abc FROM STREAM() GROUP BY abc ORDER BY AVG(def)");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.AVG,
                                                                                                  "def",
                                                                                                  "AVG(def)")));

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
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        CountDistinct aggregation = (CountDistinct) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.COUNT_DISTINCT);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getName(), "COUNT(DISTINCT abc)");
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testCountDistinctNonPrimitive() {
        build("SELECT COUNT(DISTINCT aaa, bbb) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The types of the arguments in COUNT(DISTINCT aaa, bbb) must be primitive. Types given: [STRING_MAP_LIST, STRING_MAP_MAP]");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testCountDistinctWithComputation() {
        build("SELECT COUNT(DISTINCT abc + 5) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        CountDistinct aggregation = (CountDistinct) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.COUNT_DISTINCT);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getName(), "COUNT(DISTINCT abc + 5)");
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testCountDistinctAsComputation() {
        build("SELECT COUNT(DISTINCT abc), COUNT(DISTINCT abc) + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        CountDistinct aggregation = (CountDistinct) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.COUNT_DISTINCT);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getName(), "COUNT(DISTINCT abc)");
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("COUNT(DISTINCT abc) + 5", binary(field("COUNT(DISTINCT abc)", Type.LONG),
                                                                                                                           value(5),
                                                                                                                           Operation.ADD,
                                                                                                                           Type.LONG))));
    }

    @Test
    public void testCountDistinctAsComputationOnly() {
        build("SELECT COUNT(DISTINCT abc) + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        CountDistinct aggregation = (CountDistinct) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.COUNT_DISTINCT);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getName(), "COUNT(DISTINCT abc)");
        Assert.assertEquals(query.getPostAggregations().size(), 2);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("COUNT(DISTINCT abc) + 5", binary(field("COUNT(DISTINCT abc)", Type.LONG),
                                                                                                                           value(5),
                                                                                                                           Operation.ADD,
                                                                                                                           Type.LONG))));

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("COUNT(DISTINCT abc)"));
    }

    @Test
    public void testCountDistinctAliasAsComputation() {
        build("SELECT COUNT(DISTINCT abc) AS count, COUNT(DISTINCT abc) + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        CountDistinct aggregation = (CountDistinct) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.COUNT_DISTINCT);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getName(), "count");
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("COUNT(DISTINCT abc) + 5", binary(field("count", Type.LONG),
                                                                                                                           value(5),
                                                                                                                           Operation.ADD,
                                                                                                                           Type.LONG))));
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
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        LinearDistribution aggregation = (LinearDistribution) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.DISTRIBUTION);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getDistributionType(), DistributionType.QUANTILE);
        Assert.assertEquals(aggregation.getNumberOfPoints(), 11);
        Assert.assertEquals(aggregation.getSize(), (Integer) 20);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testPMFDistribution() {
        build("SELECT FREQ(abc, REGION, 2000, 20000, 500) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        RegionDistribution aggregation = (RegionDistribution) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.DISTRIBUTION);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getDistributionType(), DistributionType.PMF);
        Assert.assertEquals(aggregation.getStart(), 2000.0);
        Assert.assertEquals(aggregation.getEnd(), 20000.0);
        Assert.assertEquals(aggregation.getIncrement(), 500.0);
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testCDFDistribution() {
        build("SELECT CUMFREQ(abc, MANUAL, 20000, 2000, 15000, 45000) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        ManualDistribution aggregation = (ManualDistribution) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.DISTRIBUTION);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getDistributionType(), DistributionType.CDF);
        // Points are sorted
        Assert.assertEquals(aggregation.getPoints(), Arrays.asList(2000.0, 15000.0, 20000.0, 45000.0));
        Assert.assertEquals(aggregation.getSize(), defaultSize);
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
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        LinearDistribution aggregation = (LinearDistribution) query.getAggregation();

        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getDistributionType(), DistributionType.QUANTILE);
        Assert.assertEquals(aggregation.getNumberOfPoints(), 11);
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopK() {
        build("SELECT TOP(10, abc) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKWithThreshold() {
        build("SELECT TOP(10, 100, abc) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getThreshold(), (Long) 100L);
        Assert.assertEquals(aggregation.getName(), AggregationExtractor.DEFAULT_TOP_K_NAME);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
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
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc"));
        Assert.assertTrue(aggregation.getFields().contains("def"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "abc");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def"), "def");
        Assert.assertEquals(aggregation.getName(), AggregationExtractor.DEFAULT_TOP_K_NAME);
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKWithComputations() {
        build("SELECT TOP(10, abc, def + 5) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("def + 5", binary(field("def", Type.FLOAT),
                                                                                                  value(5),
                                                                                                  Operation.ADD,
                                                                                                  Type.FLOAT)));

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc"));
        Assert.assertTrue(aggregation.getFields().contains("def + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "abc");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def + 5"), "def + 5");
        Assert.assertEquals(aggregation.getName(), AggregationExtractor.DEFAULT_TOP_K_NAME);
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testTopKWithAdditionalFields() {
        build("SELECT TOP(10, abc, def + 5), abc + 5, COUNT + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("def + 5", binary(field("def", Type.FLOAT),
                                                                                                  value(5),
                                                                                                  Operation.ADD,
                                                                                                  Type.FLOAT)));

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc"));
        Assert.assertTrue(aggregation.getFields().contains("def + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "abc");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def + 5"), "def + 5");
        Assert.assertEquals(aggregation.getName(), AggregationExtractor.DEFAULT_TOP_K_NAME);
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields().size(), 2);
        Assert.assertEquals(computation.getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                        value(5),
                                                                                        Operation.ADD,
                                                                                        Type.INTEGER)));
        Assert.assertEquals(computation.getFields().get(1), new Field("COUNT + 5", binary(field("COUNT", Type.LONG),
                                                                                          value(5),
                                                                                          Operation.ADD,
                                                                                          Type.LONG)));
    }

    @Test
    public void testTopKAlias() {
        build("SELECT TOP(10, abc) AS top, top + 5 FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getName(), "top");
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields().size(), 1);
        Assert.assertEquals(computation.getFields().get(0), new Field("top + 5", binary(field("top", Type.LONG),
                                                                                        value(5),
                                                                                        Operation.ADD,
                                                                                        Type.LONG)));
    }

    @Test
    public void testTopKWithAlias() {
        build("SELECT TOP(10, abc, def), abc AS one, def AS two FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc"));
        Assert.assertTrue(aggregation.getFields().contains("def"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "one");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def"), "two");
        Assert.assertEquals(aggregation.getName(), AggregationExtractor.DEFAULT_TOP_K_NAME);
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialK() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getName(), "COUNT(*)");
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithMultipleFields() {
        build("SELECT abc, def, COUNT(*) FROM STREAM() GROUP BY abc, def ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc"));
        Assert.assertTrue(aggregation.getFields().contains("def"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "abc");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def"), "def");
        Assert.assertEquals(aggregation.getName(), "COUNT(*)");
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithHaving() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING COUNT(*) >= 100 ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getName(), "COUNT(*)");
        Assert.assertEquals(aggregation.getThreshold(), (Long) 100L);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithAliasField() {
        build("SELECT abc AS def, COUNT(*) FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "def"));
        Assert.assertEquals(aggregation.getName(), "COUNT(*)");
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithAliasCount1() {
        build("SELECT abc, COUNT(*) AS count FROM STREAM() GROUP BY abc HAVING count >= 50 ORDER BY count DESC LIMIT 10");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getName(), "count");
        Assert.assertEquals(aggregation.getThreshold(), (Long) 50L);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithAliasCount2() {
        build("SELECT abc, COUNT(*) AS count FROM STREAM() GROUP BY abc HAVING COUNT(*) >= 50 ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getName(), "count");
        Assert.assertEquals(aggregation.getThreshold(), (Long) 50L);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSpecialKWithComputationAndAlias() {
        build("SELECT abc + 5 AS def, COUNT(*) FROM STREAM() GROUP BY abc + 5 ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc + 5", "def"));
        Assert.assertEquals(aggregation.getName(), "COUNT(*)");
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testNotSpecialKMissingLimit() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.COUNT,
                                                                                                  null,
                                                                                                  "COUNT(*)")));

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getField(), "COUNT(*)");
        Assert.assertEquals(orderBy.getFields().get(0).getDirection(), OrderBy.Direction.DESC);
    }

    @Test
    public void testNotSpecialKNotCount() {
        build("SELECT abc, AVG(abc) FROM STREAM() GROUP BY abc ORDER BY AVG(abc) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), AggregationType.TOP_K);
    }

    @Test
    public void testNotSpecialKFieldsNotSelected() {
        build("SELECT COUNT(*) FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), AggregationType.TOP_K);
    }

    @Test
    public void testNotSpecialKCountNotSelected() {
        build("SELECT abc FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), AggregationType.TOP_K);
    }

    @Test
    public void testNotSpecialKNotCountOrderBy() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc ORDER BY abc DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), AggregationType.TOP_K);
    }

    @Test
    public void testNotSpecialKNotOrderByDesc() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc ORDER BY COUNT(*) ASC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), AggregationType.TOP_K);
    }

    @Test
    public void testNotSpecialKBadHaving() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING COUNT(*) ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), AggregationType.TOP_K);

        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING abc >= 5 ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), AggregationType.TOP_K);

        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING COUNT(*) > 5 ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), AggregationType.TOP_K);

        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING COUNT(*) >= abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), AggregationType.TOP_K);

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
        Assert.assertEquals(field.getValue(), field("abc", Type.INTEGER));
    }

    @Test
    public void testFieldExpressionWithIndex() {
        build("SELECT aaa[0] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "aaa[0]");
        Assert.assertEquals(field.getValue(), field("aaa", 0, Type.STRING_MAP));
        Assert.assertEquals(field.getValue().getType(), Type.STRING_MAP);
    }

    @Test
    public void testFieldExpressionWithIndexAndSubKey() {
        build("SELECT aaa[0].def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "aaa[0].def");
        Assert.assertEquals(field.getValue(), field("aaa", 0, "def", Type.STRING));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testFieldExpressionWithKey() {
        build("SELECT bbb.def FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb.def");
        Assert.assertEquals(field.getValue(), field("bbb", "def", Type.STRING_MAP));
        Assert.assertEquals(field.getValue().getType(), Type.STRING_MAP);
    }

    @Test
    public void testFieldExpressionWithKeyAndSubKey() {
        build("SELECT bbb.def.one FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb.def.one");
        Assert.assertEquals(field.getValue(), field("bbb", "def", "one", Type.STRING));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testQuotedFieldExpressionWithKeyAndSubKey() {
        build("SELECT \"bbb\".\"def\".\"one\" FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "bbb.def.one");
        Assert.assertEquals(field.getValue(), field("bbb", "def", "one", Type.STRING));
        Assert.assertEquals(field.getValue().getType(), Type.STRING);
    }

    @Test
    public void testSignedNumbers() {
        build("SELECT 5 AS a, -5 AS b, 5L AS c, -5L AS d, 5.0 AS e, -5.0 AS f, 5.0f AS g, -5.0f AS h FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 8);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("a", value(5)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("b", value(-5)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("c", value(5L)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("d", value(-5L)));
        Assert.assertEquals(query.getProjection().getFields().get(4), new Field("e", value(5.0)));
        Assert.assertEquals(query.getProjection().getFields().get(5), new Field("f", value(-5.0)));
        Assert.assertEquals(query.getProjection().getFields().get(6), new Field("g", value(5.0f)));
        Assert.assertEquals(query.getProjection().getFields().get(7), new Field("h", value(-5.0f)));
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testBoolean() {
        build("SELECT true, false FROM STREAM()");
        build("SELECT true, false FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("true", value(true)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("false", value(false)));
    }

    @Test
    public void testBinaryOperations() {
        build("SELECT a + 5, a - 5, a * 5, a / 5, a = 5, a != 5, a > 5, a < 5, a >= 5, a <= 5, " +
              "RLIKE(c, 'abc'), RLIKEANY(c, ['abc']), SIZEIS(c, 5), CONTAINSKEY(bbb, 'abc'), CONTAINSVALUE(aaa, 'abc'), " +
              "'abc' IN aaa, FILTER(aaa, [true, false]), b AND true, b OR false, b XOR true FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 20);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("a + 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.ADD,
                                                                                                Type.LONG)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("a - 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.SUB,
                                                                                                Type.LONG)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("a * 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.MUL,
                                                                                                Type.LONG)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("a / 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.DIV,
                                                                                                Type.LONG)));
        Assert.assertEquals(query.getProjection().getFields().get(4), new Field("a = 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.EQUALS,
                                                                                                Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(5), new Field("a != 5", binary(field("a", Type.LONG),
                                                                                                 value(5),
                                                                                                 Operation.NOT_EQUALS,
                                                                                                 Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(6), new Field("a > 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.GREATER_THAN,
                                                                                                Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(7), new Field("a < 5", binary(field("a", Type.LONG),
                                                                                                value(5),
                                                                                                Operation.LESS_THAN,
                                                                                                Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(8), new Field("a >= 5", binary(field("a", Type.LONG),
                                                                                                 value(5),
                                                                                                 Operation.GREATER_THAN_OR_EQUALS,
                                                                                                 Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(9), new Field("a <= 5", binary(field("a", Type.LONG),
                                                                                                 value(5),
                                                                                                 Operation.LESS_THAN_OR_EQUALS,
                                                                                                 Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(10), new Field("RLIKE(c, 'abc')", binary(field("c", Type.STRING),
                                                                                                           value("abc"),
                                                                                                           Operation.REGEX_LIKE,
                                                                                                           Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(11), new Field("RLIKEANY(c, ['abc'])", binary(field("c", Type.STRING),
                                                                                                                list(Type.STRING_LIST, value("abc")),
                                                                                                                Operation.REGEX_LIKE_ANY,
                                                                                                                Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(12), new Field("SIZEIS(c, 5)", binary(field("c", Type.STRING),
                                                                                                        value(5),
                                                                                                        Operation.SIZE_IS,
                                                                                                        Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(13), new Field("CONTAINSKEY(bbb, 'abc')", binary(field("bbb", Type.STRING_MAP_MAP),
                                                                                                                   value("abc"),
                                                                                                                   Operation.CONTAINS_KEY,
                                                                                                                   Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(14), new Field("CONTAINSVALUE(aaa, 'abc')", binary(field("aaa", Type.STRING_MAP_LIST),
                                                                                                                     value("abc"),
                                                                                                                     Operation.CONTAINS_VALUE,
                                                                                                                     Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(15), new Field("'abc' IN aaa", binary(value("abc"),
                                                                                                        field("aaa", Type.STRING_MAP_LIST),
                                                                                                        Operation.IN,
                                                                                                        Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(16), new Field("FILTER(aaa, [true, false])", binary(field("aaa", Type.STRING_MAP_LIST),
                                                                                                                      list(Type.BOOLEAN_LIST, value(true), value(false)),
                                                                                                                      Operation.FILTER,
                                                                                                                      Type.STRING_MAP_LIST)));
        Assert.assertEquals(query.getProjection().getFields().get(17), new Field("b AND true", binary(field("b", Type.BOOLEAN),
                                                                                                      value(true),
                                                                                                      Operation.AND,
                                                                                                      Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(18), new Field("b OR false", binary(field("b", Type.BOOLEAN),
                                                                                                      value(false),
                                                                                                      Operation.OR,
                                                                                                      Type.BOOLEAN)));
        Assert.assertEquals(query.getProjection().getFields().get(19), new Field("b XOR true", binary(field("b", Type.BOOLEAN),
                                                                                                      value(true),
                                                                                                      Operation.XOR,
                                                                                                      Type.BOOLEAN)));
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
        build("SELECT 'foo' > ANY aaa, 5 > ANY ccc, 5 > ALL eee, 5 = ANY ccc, 5 = ALL 'foo', 5 = ANY aaa, 'foo' = ALL eee FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The left operand in 'foo' > ANY aaa must be numeric. Type given: STRING");
        Assert.assertEquals(errors.get(1).getError(), "The right operand in 'foo' > ANY aaa must be some numeric LIST. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.get(2).getError(), "The right operand in 5 > ALL eee must be some numeric LIST. Type given: STRING_LIST");
        Assert.assertEquals(errors.get(3).getError(), "The right operand in 5 = ALL 'foo' must be some LIST. Type given: STRING");
        Assert.assertEquals(errors.get(4).getError(), "The type of the left operand and the subtype of the right operand in 5 = ANY aaa must be comparable or the same. Types given: INTEGER, STRING_MAP_LIST");
        Assert.assertEquals(errors.size(), 5);
    }

    @Test
    public void testTypeCheckRegexLike() {
        build("SELECT RLIKE('foo', 0), RLIKE(0, 'foo') FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The types of the arguments in RLIKE('foo', 0) must be STRING. Types given: STRING, INTEGER");
        Assert.assertEquals(errors.get(1).getError(), "The types of the arguments in RLIKE(0, 'foo') must be STRING. Types given: INTEGER, STRING");
        Assert.assertEquals(errors.size(), 2);
    }

    @Test
    public void testTypeCheckRegexLikeAny() {
        build("SELECT RLIKEANY(0, 'foo') FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "The type of the left operand in RLIKEANY(0, 'foo') must be STRING. Type given: INTEGER");
        Assert.assertEquals(errors.get(1).getError(), "The type of the right operand in RLIKEANY(0, 'foo') must be STRING_LIST. Type given: STRING");
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
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("a", field("a", Type.STRING)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("b", field("b", Type.STRING_LIST)));
        Assert.assertEquals(query.getProjection().getFields().get(2), new Field("c", field("c", Type.STRING_MAP)));
        Assert.assertEquals(query.getProjection().getFields().get(3), new Field("d", field("d", Type.STRING_MAP_LIST)));
        Assert.assertEquals(query.getProjection().getFields().get(4), new Field("e", field("e", Type.STRING_MAP_MAP)));
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testUnaryExpressionSizeOfCollection() {
        build("SELECT SIZEOF(aaa) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "SIZEOF(aaa)");
        Assert.assertEquals(field.getValue(), unary(field("aaa", Type.STRING_MAP_LIST), Operation.SIZE_OF, Type.INTEGER));
    }

    @Test
    public void testUnaryExpressionSizeOfString() {
        build("SELECT SIZEOF(c) FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "SIZEOF(c)");
        Assert.assertEquals(field.getValue(), unary(field("c", Type.STRING), Operation.SIZE_OF, Type.INTEGER));
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
        Assert.assertEquals(field.getValue(), unary(field("abc", Type.INTEGER), Operation.NOT, Type.BOOLEAN));
    }

    @Test
    public void testUnaryExpressionNotBoolean() {
        build("SELECT NOT b FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "NOT b");
        Assert.assertEquals(field.getValue(), unary(field("b", Type.BOOLEAN), Operation.NOT, Type.BOOLEAN));
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
        Assert.assertEquals(field.getValue(), list(Type.INTEGER_LIST, field("abc", Type.INTEGER),
                                                                 value(5),
                                                                 value(10)));
        Assert.assertEquals(field.getValue().getType(), Type.INTEGER_LIST);
    }

    @Test
    public void testListExpressionSubMap() {
        build("SELECT [ddd, ddd, ddd] FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "[ddd, ddd, ddd]");
        Assert.assertEquals(field.getValue(), list(Type.STRING_MAP_LIST, field("ddd", Type.STRING_MAP),
                                                                         field("ddd", Type.STRING_MAP),
                                                                         field("ddd", Type.STRING_MAP)));
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
        Assert.assertEquals(field.getValue(), nary(Type.INTEGER, Operation.IF, field("b", Type.BOOLEAN),
                                                                               value(5),
                                                                               value(10)));
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
        Assert.assertEquals(field.getValue(), cast(field("abc", Type.INTEGER), Type.DOUBLE, Type.DOUBLE));
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
        Assert.assertEquals(field.getValue(), unary(field("abc", Type.INTEGER), Operation.IS_NULL, Type.BOOLEAN));
    }

    @Test
    public void testIsNotNull() {
        build("SELECT abc IS NOT NULL FROM STREAM() WHERE abc IS NOT NULL");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);

        Field field = query.getProjection().getFields().get(0);

        Assert.assertEquals(field.getName(), "abc IS NOT NULL");
        Assert.assertEquals(field.getValue(), unary(field("abc", Type.INTEGER), Operation.IS_NOT_NULL, Type.BOOLEAN));
    }

    @Test
    public void testNullValue() {
        build("SELECT NULL FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("NULL", value(null))));
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
        build("SELECT AVG(foo) AS bar FROM STREAM() ORDER BY bar[0]");
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
