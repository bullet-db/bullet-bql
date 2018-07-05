/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/parser/ParsingOptions.java
 */
package com.yahoo.bullet.bql.parser;

import static java.util.Objects.requireNonNull;

public class ParsingOptions {
    public enum DecimalLiteralTreatment {
        AS_DOUBLE,
        AS_DECIMAL,
        REJECT
    }

    private final DecimalLiteralTreatment decimalLiteralTreatment;

    /**
     * Constructor with a default {@link DecimalLiteralTreatment#REJECT}.
     */
    public ParsingOptions() {
        this(DecimalLiteralTreatment.REJECT);
    }

    /**
     * Constructor with a given DecimalLiteralTreatment.
     *
     * @param decimalLiteralTreatment The non-null DecimalLiteralTreatment that defines how to treat decimal number when parsing.
     */
    public ParsingOptions(DecimalLiteralTreatment decimalLiteralTreatment) {
        this.decimalLiteralTreatment = requireNonNull(decimalLiteralTreatment, "DecimalLiteralTreatment is null");
    }

    /**
     * Get the {@link #decimalLiteralTreatment} of the ParingOptions.
     *
     * @return The {@link #decimalLiteralTreatment}.
     */
    public DecimalLiteralTreatment getDecimalLiteralTreatment() {
        return decimalLiteralTreatment;
    }
}
