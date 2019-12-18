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
    //private static final Pattern NAME_PATTERN = Pattern.compile("[a-zA-Z_]([a-zA-Z0-9_])*");
    private final String value;
    //private final boolean delimited;

    /**
     * Constructor that requires a String.
     *
     * @param value A String.
     */
    public IdentifierNode(String value) {
        this.value = value;
        //this.delimited = !NAME_PATTERN.matcher(value).matches();
        //this.delimited = value.contains(".");
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitIdentifier(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof IdentifierNode && Objects.equals(value, ((IdentifierNode) obj).value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
