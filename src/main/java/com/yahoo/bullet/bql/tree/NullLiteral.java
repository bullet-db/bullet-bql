/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/NullLiteral.java
 */
package com.yahoo.bullet.bql.tree;

import java.util.Optional;

public class NullLiteral extends Literal {
    /**
     * Constructor that requires nothing.
     */
    public NullLiteral() {
        super(Optional.empty());
    }

    /**
     * Constructor that requires a {@link NodeLocation}.
     *
     * @param location A {@link NodeLocation}.
     */
    public NullLiteral(NodeLocation location) {
        super(Optional.of(location));
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitNullLiteral(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
