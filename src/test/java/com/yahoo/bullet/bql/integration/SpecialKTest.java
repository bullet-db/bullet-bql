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
import com.yahoo.bullet.query.aggregations.TopK;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class SpecialKTest extends IntegrationTest {
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
    public void testSpecialKAcceptsCountAlias() {
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
    public void testSpecialKUsesCountAliasAsName() {
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
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("COUNT(*)", Type.LONG));
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

        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING 100 <= COUNT(*) ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertNotEquals(query.getAggregation().getType(), AggregationType.TOP_K);

        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc HAVING COUNT(*) >= '5' ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(errors.get(0).getError(), "1:56: The left and right operands in COUNT(*) >= '5' must either both be numeric or both be STRING. Types given: LONG, STRING.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testNotSpecialKInvalidType() {
        build("SELECT aaa, COUNT(*) FROM STREAM() GROUP BY aaa ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(errors.get(0).getError(), "1:45: The GROUP BY field aaa is non-primitive. Type given: STRING_MAP_LIST.");
    }

    @Test
    public void testSpecialKWithAdditionalField() {
        build("SELECT abc, COUNT(*), def FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(errors.get(0).getError(), "1:23: The field def does not exist in the schema.");
    }

    @Test
    public void testNotSpecialKAdditionalGroupBy() {
        build("SELECT abc, COUNT(*) FROM STREAM() GROUP BY abc, def ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields().size(), 2);
        Assert.assertTrue(aggregation.getFields().contains("abc"));
        Assert.assertTrue(aggregation.getFields().contains("def"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 2);
        Assert.assertEquals(aggregation.getFieldsToNames().get("abc"), "abc");
        Assert.assertEquals(aggregation.getFieldsToNames().get("def"), "def");
        Assert.assertEquals(aggregation.getOperations(), Collections.singleton(new GroupOperation(GroupOperation.GroupOperationType.COUNT,
                                                                                                  null,
                                                                                                  "COUNT(*)")));

        Assert.assertEquals(query.getPostAggregations().size(), 2);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("COUNT(*)", Type.LONG));
        Assert.assertEquals(orderBy.getFields().get(0).getDirection(), OrderBy.Direction.DESC);

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("def"));
    }

    @Test
    public void testSpecialKWithComputation() {
        build("SELECT abc, COUNT(*), abc + 5 FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "abc"));
        Assert.assertEquals(aggregation.getName(), "COUNT(*)");
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields().size(), 1);
        Assert.assertEquals(computation.getFields().get(0), new Field("abc + 5", binary(field("abc", Type.INTEGER), value(5), Operation.ADD, Type.INTEGER)));
    }

    @Test
    public void testSpecialKWithComputationSubstitutesAlias() {
        build("SELECT abc AS def, COUNT(*), abc + 5 FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "def"));
        Assert.assertEquals(aggregation.getName(), "COUNT(*)");
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields().size(), 1);
        Assert.assertEquals(computation.getFields().get(0), new Field("abc + 5", binary(field("def", Type.INTEGER), value(5), Operation.ADD, Type.INTEGER)));
    }

    @Test
    public void testSpecialKWithComputationUsesAlias() {
        build("SELECT abc AS def, COUNT(*), def + 5 FROM STREAM() GROUP BY abc ORDER BY COUNT(*) DESC LIMIT 10");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        TopK aggregation = (TopK) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.TOP_K);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("abc"));
        Assert.assertEquals(aggregation.getFieldsToNames(), Collections.singletonMap("abc", "def"));
        Assert.assertEquals(aggregation.getName(), "COUNT(*)");
        Assert.assertNull(aggregation.getThreshold());
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        Computation computation = (Computation) query.getPostAggregations().get(0);

        Assert.assertEquals(computation.getFields().size(), 1);
        Assert.assertEquals(computation.getFields().get(0), new Field("def + 5", binary(field("def", Type.INTEGER), value(5), Operation.ADD, Type.INTEGER)));
    }
}
