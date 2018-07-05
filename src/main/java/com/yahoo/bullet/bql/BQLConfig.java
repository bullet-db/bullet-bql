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

import static com.yahoo.bullet.bql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.yahoo.bullet.bql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.yahoo.bullet.bql.parser.ParsingOptions.DecimalLiteralTreatment.REJECT;

@Slf4j
public class BQLConfig extends BulletConfig {
    // Common BQLParser properties.
    public static final String BQL_DECIMAL_LITERAL_TREATMENT = "bullet.bql.decimal.literal.treatment";
    public static final String BQL_DELIMITER = "bullet.bql.delimiter";

    // Defaults.
    public static final String DEFAULT_BQL_DECIMAL_LITERAL_TREATMENT = "AS_DOUBLE";
    public static final String DEFAULT_BQL_DELIMITER = ";";
    public static final String DEFAULT_BQL_CONFIGURATION = "bullet_bql_defaults.yaml";

    // Validator definitions for the configs in this class.
    // This can be static since VALIDATOR itself does not change for different values for fields in the BQLConfig.
    private static final Validator VALIDATOR = new Validator();

    static {
        VALIDATOR.define(BQL_DECIMAL_LITERAL_TREATMENT)
                 .defaultTo(DEFAULT_BQL_DECIMAL_LITERAL_TREATMENT)
                 .checkIf(Validator::isString)
                 .checkIf(Validator.isIn(AS_DOUBLE.name(), AS_DECIMAL.name(), REJECT.name()));
        VALIDATOR.define(BQL_DELIMITER)
                 .defaultTo(DEFAULT_BQL_DELIMITER)
                 .checkIf(Validator::isString);
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
        // Load default BQLConfig settings. Merge additional settings in Config.
        super(DEFAULT_BQL_CONFIGURATION);
        merge(config);
        VALIDATOR.validate(this);
        log.info("Merged settings:\n {}", this);
    }
}
