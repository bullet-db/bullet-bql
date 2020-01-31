/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.common.BulletConfig;
import org.testng.annotations.Test;

public class BQLConfigTest {
    @Test
    public void testConstructor() {
        // coverage
        new BQLConfig();
        new BQLConfig(BulletConfig.DEFAULT_CONFIGURATION_NAME);
    }
}
