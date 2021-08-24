/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import com.yahoo.bullet.bql.BQLResult;
import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.aggregations.GroupBy;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class OuterQueryTest extends IntegrationTest {
    private BulletQueryBuilder noSchemaBuilder = new BulletQueryBuilder(new BulletConfig());

    @Test
    public void testOuterQueryPassthroughSchemaFieldExists() {
        build("SELECT abc FROM (SELECT * FROM STREAM())");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertNull(query.getFilter());
        Assert.assertEquals(query.getAggregation().getSize(), defaultSize);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertEquals(query.getDuration(), defaultDuration);
        Assert.assertNotNull(query.getOuterQuery());

        Query outerQuery = query.getOuterQuery();

        Assert.assertEquals(outerQuery.getProjection().getFields().size(), 1);
        Assert.assertEquals(outerQuery.getProjection().getFields().get(0), new Field("abc", field("abc", Type.INTEGER)));
        Assert.assertEquals(outerQuery.getAggregation().getType(), AggregationType.RAW);
    }

    @Test
    public void testOuterQueryPassthroughSchemaInvalidDoesNotExist() {
        build("SELECT foo FROM (SELECT * FROM STREAM())");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The field foo does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testOuterQueryPassthroughNoSchema() {
        BQLResult result = noSchemaBuilder.buildQuery("SELECT abc FROM (SELECT * FROM STREAM())");

        query = result.getQuery();

        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertNull(query.getFilter());
        Assert.assertEquals(query.getAggregation().getSize(), defaultSize);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertEquals(query.getDuration(), defaultDuration);
        Assert.assertNotNull(query.getOuterQuery());

        Query outerQuery = query.getOuterQuery();

        Assert.assertEquals(outerQuery.getProjection().getFields().size(), 1);
        Assert.assertEquals(outerQuery.getProjection().getFields().get(0), new Field("abc", field("abc", Type.UNKNOWN)));
        Assert.assertEquals(outerQuery.getAggregation().getType(), AggregationType.RAW);
    }

    @Test
    public void testOuterQueryAggregateAsField() {
        build("SELECT color, \"COUNT(*)\" AS count FROM (SELECT c AS color, COUNT(*) FROM STREAM() GROUP BY c) WHERE \"COUNT(*)\" > 1");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("c"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("c", "color"));
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.COUNT,
                                                                                                  null,
                                                                                                  "COUNT(*)")));
        Assert.assertNull(query.getPostAggregations());
        Assert.assertNotNull(query.getOuterQuery());

        Query outerQuery = query.getOuterQuery();

        Assert.assertEquals(outerQuery.getProjection().getFields().size(), 2);
        Assert.assertEquals(outerQuery.getProjection().getFields().get(0), new Field("color", field("color", Type.STRING)));
        Assert.assertEquals(outerQuery.getProjection().getFields().get(1), new Field("count", field("COUNT(*)", Type.LONG)));
        Assert.assertEquals(outerQuery.getFilter(), binary(field("COUNT(*)", Type.LONG), value(1), Operation.GREATER_THAN, Type.BOOLEAN));
        Assert.assertEquals(outerQuery.getAggregation().getType(), AggregationType.RAW);
    }
}
