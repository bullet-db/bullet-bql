/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.aggregations.GroupAll;
import com.yahoo.bullet.query.aggregations.GroupBy;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class GroupAllTest extends IntegrationTest {
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
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the argument in AVG(aaa) must be numeric. Type given: STRING_MAP_LIST");
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
    public void testGroupOpRenameComputationToExistingOpNoCulling() {
        build("SELECT AVG(abc) + 5 AS \"AVG(abc)\" FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupAll aggregation = (GroupAll) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.emptyList());
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.AVG,
                                                                                                  "abc",
                                                                                                  "AVG(abc)")));

        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("AVG(abc)", binary(field("AVG(abc)", Type.DOUBLE),
                                                                                                            value(5),
                                                                                                            Operation.ADD,
                                                                                                            Type.DOUBLE))));
    }

    @Test
    public void testGroupOpCannotBeUsedAsFieldInComputation() {
        build("SELECT AVG(abc), \"AVG(abc)\" + 5 FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:18: The field AVG(abc) does not exist in the schema.");
    }
}
