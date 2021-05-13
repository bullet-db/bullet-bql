/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static com.yahoo.bullet.bql.util.QueryUtil.binary;
import static com.yahoo.bullet.bql.util.QueryUtil.field;
import static com.yahoo.bullet.bql.util.QueryUtil.value;

public class SelectAllTest extends IntegrationTest {
    @Test
    public void testSelectAllWithAdditionalFieldsAndOrderBy() {
        // The QueryBuilder logic for SELECT ALL previously locked the base schema when additional fields were selected.
        // This was the incorrect behavior and would make it impossible to ORDER BY a field that was in the base schema.
        // This test is here to ensure the correct behavior.
        build("SELECT *, abc + 5 FROM STREAM() ORDER BY def");
        Assert.assertEquals(query.getProjection().getFields(), Collections.singletonList(new Field("abc + 5", binary(field("abc", Type.INTEGER),
                                                                                                                     value(5),
                                                                                                                     Operation.ADD,
                                                                                                                     Type.INTEGER))));
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.COPY);
        Assert.assertEquals(query.getPostAggregations().size(), 1);

        OrderBy orderBy = (OrderBy) query.getPostAggregations().get(0);

        Assert.assertEquals(orderBy.getFields().size(), 1);
        Assert.assertEquals(orderBy.getFields().get(0).getExpression(), field("def", Type.FLOAT));
    }

    @Test
    public void testSelectAllWithTrivialAlias() {
        // The QueryBuilder logic for SELECT ALL checks to see if there are additional fields selected to determine its
        // projection type (COPY or PASS_THROUGH). It uses COPY if it decides that it must project additional fields,
        // which are non-field expressions and aliased expressions. Before, this meant that it would use COPY projection
        // even for trivially-aliased fields like the one below. However, ProcessedQuery was changed so that trivial
        // aliases are ignored; therefore, the query below is treated instead as SELECT *, abc FROM STREAM() which is
        // equivalent to SELECT * FROM STREAM() assuming abc exists.
        build("SELECT *, abc AS abc FROM STREAM()");
        Assert.assertEquals(query.getProjection().getType(), Projection.Type.PASS_THROUGH);
        Assert.assertNull(query.getPostAggregations());
    }
}
