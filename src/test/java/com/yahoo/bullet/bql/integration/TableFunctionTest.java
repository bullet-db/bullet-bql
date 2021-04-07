/*
 *  Copyright 2021, Yahoo Inc.
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
import com.yahoo.bullet.query.tablefunctions.Explode;
import com.yahoo.bullet.query.tablefunctions.LateralView;
import com.yahoo.bullet.query.tablefunctions.TableFunctionType;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.unary;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class TableFunctionTest extends IntegrationTest {
    @Test
    public void testExplodeList() {
        build("SELECT EXPLODE(ccc) AS c FROM STREAM()");
        Assert.assertEquals(query.getTableFunction().getType(), TableFunctionType.EXPLODE);
        Assert.assertFalse(query.getTableFunction().isOuter());

        Explode explode = (Explode) query.getTableFunction();

        Assert.assertEquals(explode.getField(), field("ccc", Type.INTEGER_LIST));
        Assert.assertEquals(explode.getKeyAlias(), "c");
        Assert.assertNull(explode.getValueAlias());

        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testExplodeMap() {
        build("SELECT EXPLODE(ddd) AS (key, d) FROM STREAM()");
        Assert.assertEquals(query.getTableFunction().getType(), TableFunctionType.EXPLODE);
        Assert.assertFalse(query.getTableFunction().isOuter());

        Explode explode = (Explode) query.getTableFunction();

        Assert.assertEquals(explode.getField(), field("ddd", Type.STRING_MAP));
        Assert.assertEquals(explode.getKeyAlias(), "key");
        Assert.assertEquals(explode.getValueAlias(), "d");

        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testExplodeListWithAdditionalFields() {
        build("SELECT EXPLODE(ccc) AS c, c + 5, c * 10 FROM STREAM()");
        Assert.assertEquals(query.getTableFunction().getType(), TableFunctionType.EXPLODE);
        Assert.assertFalse(query.getTableFunction().isOuter());

        Explode explode = (Explode) query.getTableFunction();

        Assert.assertEquals(explode.getField(), field("ccc", Type.INTEGER_LIST));
        Assert.assertEquals(explode.getKeyAlias(), "c");
        Assert.assertNull(explode.getValueAlias());

        Assert.assertEquals(query.getProjection().getType(), Projection.Type.COPY);
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertNull(query.getPostAggregations());

        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("c + 5", binary(field("c", Type.INTEGER),
                                                                                                value(5),
                                                                                                Operation.ADD,
                                                                                                Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("c * 10", binary(field("c", Type.INTEGER),
                                                                                                 value(10),
                                                                                                 Operation.MUL,
                                                                                                 Type.INTEGER)));
    }

    @Test
    public void testExplodeMapWithAdditionalFields() {
        build("SELECT EXPLODE(ddd) AS (key, d), SIZEOF(key), TRIM(d) FROM STREAM()");
        Assert.assertEquals(query.getTableFunction().getType(), TableFunctionType.EXPLODE);
        Assert.assertFalse(query.getTableFunction().isOuter());

        Explode explode = (Explode) query.getTableFunction();

        Assert.assertEquals(explode.getField(), field("ddd", Type.STRING_MAP));
        Assert.assertEquals(explode.getKeyAlias(), "key");
        Assert.assertEquals(explode.getValueAlias(), "d");

        Assert.assertEquals(query.getProjection().getType(), Projection.Type.COPY);
        Assert.assertEquals(query.getProjection().getFields().size(), 2);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertNull(query.getPostAggregations());

        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("SIZEOF(key)", unary(field("key", Type.STRING),
                                                                                                     Operation.SIZE_OF,
                                                                                                     Type.INTEGER)));
        Assert.assertEquals(query.getProjection().getFields().get(1), new Field("TRIM(d)", unary(field("d", Type.STRING),
                                                                                                 Operation.TRIM,
                                                                                                 Type.STRING)));
    }

    @Test
    public void testExplodeOuter() {
        build("SELECT EXPLODE_OUTER(ccc) AS c FROM STREAM()");
        Assert.assertEquals(query.getTableFunction().getType(), TableFunctionType.EXPLODE);
        Assert.assertTrue(query.getTableFunction().isOuter());

        Explode explode = (Explode) query.getTableFunction();

        Assert.assertEquals(explode.getField(), field("ccc", Type.INTEGER_LIST));
        Assert.assertEquals(explode.getKeyAlias(), "c");
        Assert.assertNull(explode.getValueAlias());

        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testSelectTableFunctionWithAggregationInvalid() {
        build("SELECT DISTINCT EXPLODE(abc) AS foo FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Selecting a table function is not supported with other aggregation types.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testSelectTableFunctionWithAggregateInvalid() {
        build("SELECT EXPLODE(AVG(abc)) AS foo FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Selecting a table function is not supported with other aggregation types.");
        Assert.assertEquals(errors.get(1).getError(), "Table functions cannot contain aggregates.");
        Assert.assertEquals(errors.size(), 2);
    }

    @Test
    public void testMultipleSelectTableFunctionsInvalid() {
        build("SELECT EXPLODE(abc) AS foo, EXPLODE(def) AS bar FROM STREAM()");
        Assert.assertEquals(errors.get(0).getError(), "Cannot select multiple table functions.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testSelectSameTableFunctionValid() {
        build("SELECT EXPLODE(ccc) AS c, EXPLODE(ccc) AS c FROM STREAM()");
        Assert.assertEquals(query.getTableFunction().getType(), TableFunctionType.EXPLODE);
        Assert.assertFalse(query.getTableFunction().isOuter());

        Explode explode = (Explode) query.getTableFunction();

        Assert.assertEquals(explode.getField(), field("ccc", Type.INTEGER_LIST));
        Assert.assertEquals(explode.getKeyAlias(), "c");
        Assert.assertNull(explode.getValueAlias());

        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testExplodeWhereBaseSchemaInaccessible() {
        build("SELECT EXPLODE(eee) AS exp FROM STREAM() WHERE eee IS NOT NULL");
        Assert.assertEquals(errors.get(0).getError(), "1:48: The field eee does not exist in the schema.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testExplodeWhereExplodedFieldAccessible() {
        build("SELECT EXPLODE(eee) AS foo FROM STREAM() WHERE SIZEOF(foo) > 0");
        Assert.assertEquals(query.getTableFunction().getType(), TableFunctionType.EXPLODE);
        Assert.assertFalse(query.getTableFunction().isOuter());

        Explode explode = (Explode) query.getTableFunction();

        Assert.assertEquals(explode.getField(), field("eee", Type.STRING_LIST));
        Assert.assertEquals(explode.getKeyAlias(), "foo");
        Assert.assertNull(explode.getValueAlias());

        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertEquals(query.getFilter(), binary(unary(field("foo", Type.STRING),
                                                            Operation.SIZE_OF,
                                                            Type.INTEGER),
                                                      value(0),
                                                      Operation.GREATER_THAN,
                                                      Type.BOOLEAN));
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testExplodeWithOrderBy() {
        build("SELECT EXPLODE(eee) AS foo FROM STREAM() ORDER BY SIZEOF(foo)");
        Assert.assertEquals(query.getTableFunction().getType(), TableFunctionType.EXPLODE);
        Assert.assertFalse(query.getTableFunction().isOuter());

        Explode explode = (Explode) query.getTableFunction();

        Assert.assertEquals(explode.getField(), field("eee", Type.STRING_LIST));
        Assert.assertEquals(explode.getKeyAlias(), "foo");
        Assert.assertNull(explode.getValueAlias());

        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), unary(field("foo", Type.STRING),
                                                                              Operation.SIZE_OF,
                                                                              Type.INTEGER));
    }

    @Test
    public void testLateralViewExplode() {
        build("SELECT foo FROM STREAM() LATERAL VIEW OUTER EXPLODE(eee) AS foo");

        LateralView lateralView = (LateralView) query.getTableFunction();
        Explode explode = (Explode) lateralView.getTableFunction();

        Assert.assertEquals(lateralView.getType(), TableFunctionType.LATERAL_VIEW);
        Assert.assertTrue(lateralView.isOuter());
        Assert.assertEquals(explode.getType(), TableFunctionType.EXPLODE);
        Assert.assertFalse(explode.isOuter());
        Assert.assertEquals(explode.getField(), field("eee", Type.STRING_LIST));
        Assert.assertEquals(explode.getKeyAlias(), "foo");
        Assert.assertNull(explode.getValueAlias());

        Assert.assertEquals(query.getProjection().getType(), Projection.Type.NO_COPY);
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("foo", field("foo", Type.STRING)));
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testLateralViewWithSelectTableFunctionInvalid() {
        build("SELECT EXPLODE(foo) AS bar FROM STREAM() LATERAL VIEW EXPLODE(bbb) AS foo");
        Assert.assertEquals(errors.get(0).getError(), "Selecting a table function is not supported in queries with a LATERAL VIEW clause.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testLateralViewTableFunctionWithAggregateInvalid() {
        build("SELECT foo FROM STREAM() LATERAL VIEW EXPLODE(AVG(abc)) AS foo");
        Assert.assertEquals(errors.get(0).getError(), "Table functions cannot contain aggregates.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testLateralViewWithAggregationValid() {
        build("SELECT DISTINCT foo FROM STREAM() LATERAL VIEW EXPLODE(ccc) AS foo");

        LateralView lateralView = (LateralView) query.getTableFunction();
        Explode explode = (Explode) lateralView.getTableFunction();

        Assert.assertEquals(lateralView.getType(), TableFunctionType.LATERAL_VIEW);
        Assert.assertFalse(lateralView.isOuter());
        Assert.assertEquals(explode.getType(), TableFunctionType.EXPLODE);
        Assert.assertFalse(explode.isOuter());
        Assert.assertEquals(explode.getField(), field("ccc", Type.INTEGER_LIST));
        Assert.assertEquals(explode.getKeyAlias(), "foo");
        Assert.assertNull(explode.getValueAlias());

        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);

        GroupBy aggregation = (GroupBy) query.getAggregation();

        Assert.assertEquals(aggregation.getType(), AggregationType.GROUP);
        Assert.assertEquals(aggregation.getFields(), Collections.singletonList("foo"));
        Assert.assertEquals(aggregation.getFieldsToNames().size(), 1);
        Assert.assertEquals(aggregation.getFieldsToNames().get("foo"), "foo");
        Assert.assertTrue(aggregation.getOperations().isEmpty());
        Assert.assertEquals(aggregation.getSize(), defaultSize);
        Assert.assertNull(query.getPostAggregations());
    }

    @Test
    public void testLateralViewExplodeWhere() {
        build("SELECT foo FROM STREAM() LATERAL VIEW EXPLODE(eee) AS foo WHERE eee IS NOT NULL");

        LateralView lateralView = (LateralView) query.getTableFunction();
        Explode explode = (Explode) lateralView.getTableFunction();

        Assert.assertEquals(lateralView.getType(), TableFunctionType.LATERAL_VIEW);
        Assert.assertFalse(lateralView.isOuter());
        Assert.assertEquals(explode.getType(), TableFunctionType.EXPLODE);
        Assert.assertFalse(explode.isOuter());
        Assert.assertEquals(explode.getField(), field("eee", Type.STRING_LIST));
        Assert.assertEquals(explode.getKeyAlias(), "foo");
        Assert.assertNull(explode.getValueAlias());

        Assert.assertEquals(query.getProjection().getType(), Projection.Type.NO_COPY);
        Assert.assertEquals(query.getProjection().getFields().size(), 1);
        Assert.assertEquals(query.getProjection().getFields().get(0), new Field("foo", field("foo", Type.STRING)));
        Assert.assertEquals(query.getFilter(), unary(field("eee", Type.STRING_LIST), Operation.IS_NOT_NULL, Type.BOOLEAN));
        Assert.assertEquals(query.getAggregation().getType(), AggregationType.RAW);
        Assert.assertNull(query.getPostAggregations());
    }
}
