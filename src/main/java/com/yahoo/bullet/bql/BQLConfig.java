/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.common.Validator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BQLConfig extends BulletConfig {
    // Settings
    public static final String BQL_MAX_QUERY_LENGTH = "bullet.bql.max.query.length";

    // Defaults
    public static final int DEFAULT_BQL_MAX_QUERY_LENGTH = Integer.MAX_VALUE;

    // Default configuration
    private static final String DEFAULT_BQL_CONFIGURATION = "bullet_bql_defaults.yaml";

    // Validations
    private static final Validator VALIDATOR = BulletConfig.getValidator();

    static {
        VALIDATOR.define(BQL_MAX_QUERY_LENGTH)
                 .defaultTo(DEFAULT_BQL_MAX_QUERY_LENGTH)
                 .checkIf(Validator::isPositive)
                 .castTo(Validator::asInt);
    }

    /**
     * Constructor that loads the defaults.
     */
    public BQLConfig() {
        super(DEFAULT_BQL_CONFIGURATION);
        VALIDATOR.validate(this);
    }

    /**
     * Creates a BQLConfig by reading in a file.
     *
     * @param file The file to read in to create the BQLConfig.
     */
    public BQLConfig(String file) {
        this(new Config(file));
    }

    /**
     * Creates a BQLConfig from a Config.
     *
     * @param config The {@link Config} to copy settings from.
     */
    public BQLConfig(Config config) {
        super(DEFAULT_BQL_CONFIGURATION);
        merge(config);
        VALIDATOR.validate(this);
    }

    @Override
    public BulletConfig validate() {
        VALIDATOR.validate(this);
        return this;
    }
}
