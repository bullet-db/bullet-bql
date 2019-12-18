/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/DoubleLiteral.java
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.Objects;

@Getter
public class DoubleLiteralNode extends LiteralNode {
    private final Double value;

    /**
     * Constructor that requires a String value.
     *
     * @param value A String.
     */
    public DoubleLiteralNode(String value) {
        this.value = Double.parseDouble(value);
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitDoubleLiteral(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof DoubleLiteralNode && Objects.equals(value, ((DoubleLiteralNode) obj).value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
