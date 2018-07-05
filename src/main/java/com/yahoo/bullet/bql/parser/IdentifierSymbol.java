/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/parser/IdentifierSymbol.java
 */
package com.yahoo.bullet.bql.parser;

public enum IdentifierSymbol {
    COLON(':'),
    AT_SIGN('@');

    private final char symbol;

    IdentifierSymbol(char symbol) {
        this.symbol = symbol;
    }

    /**
     * Get the {@link #symbol}.
     *
     * @return A symbol char.
     */
    public char getSymbol() {
        return symbol;
    }
}
