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

@RequiredArgsConstructor
public final class NodeLocation {
    private final int line;
    private final int charPositionInLine;

    /**
     * Get the line number of this NodeLocation.
     *
     * @return An int.
     */
    public int getLineNumber() {
        return line;
    }

    /**
     * Get the column number of this NodeLocation.
     *
     * @return An int.
     */
    public int getColumnNumber() {
        return charPositionInLine + 1;
    }
}