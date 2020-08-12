/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/Identifier.java
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.Objects;

@Getter
public class IdentifierNode extends ExpressionNode {
    private final String value;
    private final boolean quoted;

    public IdentifierNode(String value, boolean quoted, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.value = value;
        this.quoted = quoted;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitIdentifier(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof IdentifierNode)) {
            return false;
        }
        IdentifierNode other = (IdentifierNode) obj;
        return Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
