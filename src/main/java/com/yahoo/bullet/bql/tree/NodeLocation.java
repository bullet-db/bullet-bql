/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/NodeLocation.java
 */
package com.yahoo.bullet.bql.tree;

import lombok.RequiredArgsConstructor;

import static java.lang.String.format;

@RequiredArgsConstructor
public final class NodeLocation {
    private final int line;
    private final int charPositionInLine;

    @Override
    public String toString() {
        return format("Line %s:%s: ", line, charPositionInLine + 1);
    }
}