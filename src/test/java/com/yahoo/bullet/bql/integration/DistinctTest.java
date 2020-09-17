/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.aggregations.GroupBy;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class DistinctTest extends IntegrationTest {
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
        Assert.assertEquals(errors.get(0).getError(), "1:17: The SELECT DISTINCT field aaa is non-primitive. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.get(1).getError(), "1:22: The SELECT DISTINCT field bbb is non-primitive. Type given: STRING_MAP_MAP");
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
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 1);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "abc");
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("abc", Type.INTEGER));
    }

    @Test
    public void testDistinctWithInvalidOrderByField() {
        build("SELECT DISTINCT abc FROM STREAM() ORDER BY def");
        Assert.assertEquals(errors.get(0).getError(), "1:44: The field def does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testDistinctWithOrderByComputation() {
        build("SELECT DISTINCT abc FROM STREAM() ORDER BY abc + 5");
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), binary(field("abc", Type.INTEGER),
                                                                               value(5),
                                                                               Operation.ADD,
                                                                               Type.INTEGER));
    }

    @Test
    public void testDistinctWithOrderByComputationWithoutFieldInSchema() {
        build("SELECT DISTINCT abc + 5 FROM STREAM() ORDER BY abc + 5");
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
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("abc + 5", Type.INTEGER));
    }

    @Test
    public void testDistinctWithOrderByComplexFieldNameInvalid() {
        build("SELECT DISTINCT abc + 5 FROM STREAM() ORDER BY \"abc + 5\"");
        Assert.assertEquals(errors.get(0).getError(), "1:48: The field abc + 5 does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testDistinctWithOrderByComputationAlias() {
        build("SELECT DISTINCT abc + 5 AS def FROM STREAM() ORDER BY def");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                  value(5),
                                                                                                  Operation.ADD,
                                                                                                  Type.INTEGER)));

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 1);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc + 5"), "def");
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("def", Type.INTEGER));
    }

    @Test
    public void testDistinctWithOrderByComputationUsesAlias() {
        build("SELECT DISTINCT abc + 5, def AS abc FROM STREAM() ORDER BY abc + 5");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                  value(5),
                                                                                                  Operation.ADD,
                                                                                                  Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("def", field("def", Type.FLOAT)));

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc + 5"));
        Assert.assertTrue(aggregation.getFields().contains("def"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc + 5"), "abc + 5");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def"), "abc");
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), binary(field("abc", Type.FLOAT),
                                                                               value(5),
                                                                               Operation.ADD,
                                                                               Type.FLOAT));
    }

    @Test
    public void testDistinctWithOrderByComputationSubstitutesAlias() {
        build("SELECT DISTINCT abc + 5, def AS abc FROM STREAM() ORDER BY def + 5");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                  value(5),
                                                                                                  Operation.ADD,
                                                                                                  Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("def", field("def", Type.FLOAT)));

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc + 5"));
        Assert.assertTrue(aggregation.getFields().contains("def"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc + 5"), "abc + 5");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def"), "abc");
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), binary(field("abc", Type.FLOAT),
                                                                               value(5),
                                                                               Operation.ADD,
                                                                               Type.FLOAT));
    }

    @Test
    public void testDistinctMultipleAggregates() {
        build("SELECT DISTINCT AVG(abc) FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Query does not match exactly one query type: [SELECT_DISTINCT, GROUP]");
        Assert.assertEquals(errors.size(), 1);
    }
}
