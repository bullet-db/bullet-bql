/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/QueryUtil.java
 */
package com.yahoo.bullet.bql.util;

import com.yahoo.bullet.bql.tree.IdentifierNode;

public final class QueryUtil {
    public static IdentifierNode identifier(String name) {
        return new IdentifierNode(name, false);
    }

    public static IdentifierNode quotedIdentifier(String name) {
        return new IdentifierNode(name, true);
    }
}
