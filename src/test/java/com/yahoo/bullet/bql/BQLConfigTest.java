/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.common.BulletConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BQLConfigTest {
    @Test
    public void testConstructor() {
        // coverage
        new BQLConfig(BulletConfig.DEFAULT_CONFIGURATION_NAME);
    }

    @Test
    public void testDefaultMaxQueryLength() {
        BQLConfig config = new BQLConfig();
        Assert.assertEquals(config.getAs(BQLConfig.BQL_MAX_QUERY_LENGTH, Integer.class), (Integer) BQLConfig.DEFAULT_BQL_MAX_QUERY_LENGTH);
    }

    @Test
    public void testValidateMaxQueryLength() {
        BQLConfig config = new BQLConfig();
        config.set(BQLConfig.BQL_MAX_QUERY_LENGTH, 255.5);
        config.validate();
        Assert.assertEquals(config.getAs(BQLConfig.BQL_MAX_QUERY_LENGTH, Integer.class), (Integer) 255);

        config.set(BQLConfig.BQL_MAX_QUERY_LENGTH, 0);
        config.validate();
        Assert.assertEquals(config.getAs(BQLConfig.BQL_MAX_QUERY_LENGTH, Integer.class), (Integer) BQLConfig.DEFAULT_BQL_MAX_QUERY_LENGTH);
    }
}
