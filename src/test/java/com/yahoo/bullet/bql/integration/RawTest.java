/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.query.postaggregations.PostAggregationType;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class RawTest extends IntegrationTest {
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
        Assert.assertEquals(errors.get(0).getError(), "1:8: The field foo does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testSubFieldTypeInvalid() {
        build("SELECT abc[0] FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The subfield abc[0] is invalid since the field abc has type: INTEGER.");
        Assert.assertEquals(errors.size(), 1);

        build("SELECT ccc[0].def FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The subfield ccc[0].def is invalid since the field ccc[0] has type: INTEGER.");
        Assert.assertEquals(errors.size(), 1);

        build("SELECT aaa.\"0\" FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The subfield aaa.0 is invalid since the field aaa has type: STRING_MAP_LIST.");
        Assert.assertEquals(errors.size(), 1);

        build("SELECT ddd[0] FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The subfield ddd[0] is invalid since the field ddd has type: STRING_MAP.");
        Assert.assertEquals(errors.size(), 1);

        build("SELECT ccc[c] FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the index in the subfield ccc[c] must be INTEGER or LONG. Type given: STRING.");
        Assert.assertEquals(errors.size(), 1);

        build("SELECT ddd[abc] FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the key in the subfield ddd[abc] must be STRING. Type given: INTEGER.");
        Assert.assertEquals(errors.size(), 1);

        build("SELECT aaa[0][abc] FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "1:8: The type of the subkey in the subfield aaa[0][abc] must be STRING. Type given: INTEGER.");
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
    public void testRawAliasesClash() {
        build("SELECT abc, def AS abc, aaa, bbb AS aaa, ccc FROM STREAM()");
        Assert.assertTrue(errors.get(0).getError().equals("The following field names/aliases are shared: [abc, aaa].") ||
                          errors.get(0).getError().equals("The following field names/aliases are shared: [aaa, abc]."));
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
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.CULLING);

        Culling culling = (Culling) query.getPostAggregations().get(0);

        Assert.assertEquals(culling.getTransientFields().size(), 1);
        Assert.assertTrue(culling.getTransientFields().contains("abc"));
    }

    @Test
    public void testRawAllWithAliasNameSwapped() {
        build("SELECT *, abc AS def, def AS abc FROM STREAM()");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("def", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("abc", field("def", Type.FLOAT)));
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
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("abc", Type.INTEGER));
    }

    @Test
    public void testRawAllWithOrderByNonPrimitiveNotAllowed() {
        build("SELECT * FROM STREAM() ORDER BY aaa");
        Assert.assertEquals(errors.get(0).getError(), "1:33: ORDER BY contains a non-primitive field: aaa.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testRawAllWithOrderByComputation() {
        build("SELECT * FROM STREAM() ORDER BY abc + 5");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), binary(field("abc", Type.INTEGER),
                                                                               value(5),
                                                                               Operation.ADD,
                                                                               Type.INTEGER));
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
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("abc", Type.INTEGER));
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
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("def", Type.FLOAT));

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields().size(), 1);
        Assert.assertTrue(culling.getTransientFields().contains("def"));
    }

    @Test
    public void testRawWithOrderByMultiple() {
        build("SELECT abc FROM STREAM() ORDER BY abc + 5 ASC, def DESC");
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("abc", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("def", field("def", Type.FLOAT)));
        Assert.assertEquals(query.getPostAggregations().size(), 2);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);
        Assert.assertEquals(query.getPostAggregations().get(1).getType(), PostAggregationType.CULLING);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 2);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), binary(field("abc", Type.INTEGER),
                                                                               value(5),
                                                                               Operation.ADD,
                                                                               Type.INTEGER));
        Assert.assertEquals(orderBy.getFields().get(0).getDirection(), OrderBy.Direction.ASC);
        Assert.assertEquals(orderBy.getFields().get(1).getExpression(), field("def", Type.FLOAT));
        Assert.assertEquals(orderBy.getFields().get(1).getDirection(), OrderBy.Direction.DESC);

        Culling culling = (Culling) query.getPostAggregations().get(1);

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("def"));
    }

    @Test
    public void testRawAliasWithOrderBy() {
        build("SELECT abc AS def FROM STREAM() ORDER BY abc");
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("def", field("abc", Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.NO_COPY);
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("def", Type.INTEGER));
    }

    @Test
    public void testRawAliasWithOrderByAlias() {
        build("SELECT abc AS def FROM STREAM() ORDER BY def");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("def", field("abc", Type.INTEGER))));
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("def", Type.INTEGER));
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
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("abc", Type.FLOAT));
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
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("def", Type.INTEGER));
    }

    @Test
    public void testRawAliasWithOrderByNoNestedAlias() {
        build("SELECT abc AS def, def AS abc FROM STREAM() ORDER BY abc + 5");
        Assert.assertEquals(query.getProjection().getFields(), Arrays.asList(new Field("def", field("abc", Type.INTEGER)),
                                                                             new Field("abc", field("def", Type.FLOAT))));
        Assert.assertEquals(query.getPostAggregations().size(), 1);
        Assert.assertEquals(query.getPostAggregations().get(0).getType(), PostAggregationType.ORDER_BY);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), binary(field("abc", Type.FLOAT), value(5), Operation.ADD, Type.FLOAT));
    }

    @Test
    public void testRawAliasWithOrderBySubstituteSubfield() {
        build("SELECT ccc AS def FROM STREAM() ORDER BY ccc[0]");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("def", field("ccc", Type.INTEGER_LIST))));
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("def", 0, Type.INTEGER));
    }
}
