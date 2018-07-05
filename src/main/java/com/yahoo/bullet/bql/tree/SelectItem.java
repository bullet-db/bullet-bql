/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/SelectItem.java
 */
package com.yahoo.bullet.bql.tree;

import java.util.Optional;

public abstract class SelectItem extends Node {
    public enum Type {
        // TOP(k, threshold, fields)
        TOP_K,

        // QUANTILE, FREQ, CUMFREQ
        DISTRIBUTION,

        // COUNT(DISTINCT fields)
        COUNT_DISTINCT,

        // COUNT(*), MIN(fields), MAX(fields), SUM(fields), AVG(fields)
        GROUP,

        // field
        COLUMN,

        // field.s
        SUB_COLUMN,

        // field.*
        SUB_ALL,

        // SELECT *
        ALL,

        // Unsupported selectItem
        NON_SELECT
    }

    /**
     * Constructor that requires a {@link NodeLocation}.
     *
     * @param location A {@link NodeLocation}.
     */
    protected SelectItem(Optional<NodeLocation> location) {
        super(location);
    }

    /**
     * Get the alias of this SelectItem.
     *
     * @return An Optional of {@link Identifier}.
     */
    public abstract Optional<Identifier> getAlias();

    /**
     * Get the {@link Type} of this SelectItem.
     *
     * @return A {@link Type}.
     */
    public abstract Type getType();

    /**
     * Get the {@link Expression} value of this SelectItem.
     *
     * @return An {@link Expression}.
     */
    public abstract Expression getValue();
}
