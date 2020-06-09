/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.aggregations.Raw;
import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.typesystem.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class QueryValidatorTest {
    /*
    @Test
    public void testInvalidComputation() {
        ProcessedQuery processedQuery = new ProcessedQuery();
        processedQuery.getQueryTypeSet().add(ProcessedQuery.QueryType.SELECT);
        processedQuery.setComputation(Collections.emptyList());


        Schema schema = new Schema();

        Computation computation = new Computation(Arrays.asList(new Field("abc", new ValueExpression(5))));

        Query query = new Query(new Projection(),
                                null,
                                new Raw(null),
                                Arrays.asList(computation),
                                new Window(),
                                null);

        List<BulletError> errors = QueryValidator.validate(processedQuery, query, schema);

        Assert.assertEquals(errors.get(0).getError(), "A query with RAW aggregation and PASS_THROUGH projection cannot have a COMPUTATION post-aggregation.");
        Assert.assertEquals(errors.size(), 1);
    }

    @Test
    public void testInvalidCulling() {
        ProcessedQuery processedQuery = new ProcessedQuery();
        processedQuery.getQueryTypeSet().add(ProcessedQuery.QueryType.SELECT);

        Schema schema = new Schema();

        Culling culling = new Culling(Collections.singleton("abc"));

        Query query = new Query(new Projection(),
                                null,
                                new Raw(null),
                                Arrays.asList(culling),
                                new Window(),
                                null);

        List<BulletError> errors = QueryValidator.validate(processedQuery, query, schema);

        Assert.assertEquals(errors.get(0).getError(), "CULLING contains a non-existent field: abc");
        Assert.assertEquals(errors.get(1).getError(), "A query with RAW aggregation and PASS_THROUGH projection cannot have a CULLING post-aggregation.");
        Assert.assertEquals(errors.size(), 2);
    }
    */
}
