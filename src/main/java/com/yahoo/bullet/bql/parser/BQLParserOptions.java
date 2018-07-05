/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/parser/SqlParserOptions.java
 */
package com.yahoo.bullet.bql.parser;

import com.google.common.collect.Iterables;

import java.util.EnumSet;

import static java.util.Objects.requireNonNull;

public class BQLParserOptions {
    private final EnumSet<IdentifierSymbol> allowedIdentifierSymbols = EnumSet.noneOf(IdentifierSymbol.class);

    /**
     * Get allowed {@link IdentifierSymbol} in this BQLParserOptions.
     *
     * @return An EnumSet of {@link IdentifierSymbol}.
     */
    public EnumSet<IdentifierSymbol> getAllowedIdentifierSymbols() {
        return EnumSet.copyOf(allowedIdentifierSymbols);
    }

    /**
     * Add {@link IdentifierSymbol} to {@link #allowedIdentifierSymbols}.
     *
     * @param identifierSymbols An Iterable of new {@link IdentifierSymbol}.
     * @return This BQLParserOptions with new {@link IdentifierSymbol}.
     */
    public BQLParserOptions allowIdentifierSymbol(Iterable<IdentifierSymbol> identifierSymbols) {
        Iterables.addAll(allowedIdentifierSymbols, identifierSymbols);
        return this;
    }

    /**
     * Add {@link IdentifierSymbol} to {@link #allowedIdentifierSymbols}.
     *
     * @param identifierSymbols A variable number of non-null {@link IdentifierSymbol}.
     * @return This BQLParserOptions with new {@link IdentifierSymbol}.
     */
    public BQLParserOptions allowIdentifierSymbol(IdentifierSymbol... identifierSymbols) {
        for (IdentifierSymbol identifierSymbol : identifierSymbols) {
            allowedIdentifierSymbols.add(requireNonNull(identifierSymbol, "IdentifierSymbol is null"));
        }
        return this;
    }
}
