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
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.query.postaggregations.Having;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class GroupByTest extends IntegrationTest {
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
        Assert.assertEquals(errors.get(0).getError(), "1:40: The GROUP BY field aaa is non-primitive. Type given: STRING_MAP_LIST");
        Assert.assertEquals(errors.get(1).getError(), "1:45: The GROUP BY field bbb is non-primitive. Type given: STRING_MAP_MAP");
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
    public void testGroupByWithOrderByComputationWithoutFieldInSchema() {
        // This ORDER BY clause is originally dependent on the field "abc" (which does not exist after the GROUP BY)
        build("SELECT abc + 5 FROM STREAM() GROUP BY abc + 5 ORDER BY abc + 5");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc + 5", "abc + 5"));
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("abc + 5", Type.INTEGER));
    }

    @Test
    public void testGroupByWithOrderBySubstitutesComputationWithoutFieldInSchema() {
        // This ORDER BY clause is originally dependent on the field "abc" (which does not exist after the GROUP BY)
        build("SELECT abc + 5 FROM STREAM() GROUP BY abc + 5 ORDER BY (abc + 5) * 10");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc + 5"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc + 5", "abc + 5"));
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), binary(field("abc + 5", Type.INTEGER),
                                                                               value(10),
                                                                               Operation.MUL,
                                                                               Type.INTEGER));
    }

    @Test
    public void testGroupByWithComputationAndOrderBy() {
        build("SELECT abc + 5 FROM STREAM() GROUP BY abc ORDER BY abc + 5");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(query.getPostAggregations().size(), 3);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields().size(), 1);
        Assert.assertEquals(computation.getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                        value(5),
                                                                                        Operation.ADD,
                                                                                        Type.INTEGER)));

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(1);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), binary(field("abc", Type.INTEGER),
                                                                               value(5),
                                                                               Operation.ADD,
                                                                               Type.INTEGER));

        Culling culling = (Culling) query.getPostAggregations().get(2);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("abc"));
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
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("def", Type.INTEGER));
    }

    @Test
    public void testGroupByWithComputationAndOrderByComputation() {
        build("SELECT abc + 5 FROM STREAM() GROUP BY abc ORDER BY (abc + 5) * 10");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(query.getPostAggregations().size(), 3);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields().size(), 1);
        Assert.assertEquals(computation.getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                        value(5),
                                                                                        Operation.ADD,
                                                                                        Type.INTEGER)));

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(1);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), binary(binary(field("abc", Type.INTEGER),
                                                                                      value(5),
                                                                                      Operation.ADD,
                                                                                      Type.INTEGER),
                                                                               value(10),
                                                                               Operation.MUL,
                                                                               Type.INTEGER));

        Culling culling = (Culling) query.getPostAggregations().get(2);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("abc"));
    }

    @Test
    public void testGroupByAggregatesNotAllowed() {
        build("SELECT AVG(abc) FROM STREAM() GROUP BY AVG(abc)");
        Assert.assertEquals(errors.get(0).getError(), "GROUP BY clause cannot contain aggregates.");
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
        Assert.assertEquals(errors.get(0).getError(), "1:51: HAVING clause cannot be casted to BOOLEAN: [abc]");
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
    public void testGroupBySubstituteFieldAlias() {
        build("SELECT abc AS def FROM STREAM() GROUP BY abc HAVING abc >= 5");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "def"));
        Assert.assertTrue(aggregation.getOperations().isEmpty());

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Having having = (Having) query.getPostAggregations().get(0);

        Assert.assertEquals(having.getExpression(), binary(field("def", Type.INTEGER),
                                                           value(5),
                                                           Operation.GREATER_THAN_OR_EQUALS,
                                                           Type.BOOLEAN));
    }

    @Test
    public void testGroupByHavingWithFieldAlias() {
        build("SELECT abc AS def, AVG(abc) FROM STREAM() GROUP BY abc HAVING def >= 5");
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
    public void testGroupByHavingSubstituteAggregateAlias() {
        build("SELECT AVG(abc) AS avg FROM STREAM() GROUP BY abc HAVING AVG(abc) >= 5");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.AVG,
                                                                                                  "abc",
                                                                                                  "AVG(abc)")));

        Assert.assertEquals(query.getPostAggregations().size(), 2);

        Having having = (Having) query.getPostAggregations().get(0);

        Assert.assertEquals(having.getExpression(), binary(field("avg", Type.DOUBLE),
                                                           value(5),
                                                           Operation.GREATER_THAN_OR_EQUALS,
                                                           Type.BOOLEAN));

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("abc"));
    }

    @Test
    public void testGroupByHavingWithAggregateAlias() {
        build("SELECT abc, AVG(abc) AS avg FROM STREAM() GROUP BY abc HAVING avg >= 5");
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
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("AVG(def)", Type.DOUBLE));

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("AVG(def)"));
    }
}
