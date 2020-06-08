/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.integration;

import com.yahoo.bullet.bql.BQLResult;
import com.yahoo.bullet.bql.BulletQueryBuilder;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.query.Query;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;

import java.util.List;

public abstract class IntegrationTest {
    protected BulletQueryBuilder builder;
    protected Query query;
    protected List<BulletError> errors;
    protected Integer defaultSize;
    protected Long defaultDuration;

    @BeforeClass
    public void setup() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.RECORD_SCHEMA_FILE_NAME, "test_schema.json");
        config.validate();
        builder = new BulletQueryBuilder(config);
        defaultSize = config.getAs(BulletConfig.AGGREGATION_DEFAULT_SIZE, Integer.class);
        defaultDuration = config.getAs(BulletConfig.QUERY_DEFAULT_DURATION, Long.class);
    }

    protected void build(String bql) {
        BQLResult result = builder.buildQuery(bql);
        query = result.getQuery();
        errors = result.getErrors();
        if (query != null) {
            // Check that query is indeed serializable
            Assert.assertNotNull(SerializerDeserializer.toBytes(query));
        }
    }
}
