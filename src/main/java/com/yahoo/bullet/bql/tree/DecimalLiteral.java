/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/DecimalLiteral.java
 */
package com.yahoo.bullet.bql.tree;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DecimalLiteral extends Literal {
    private final String value;

    /**
     * Constructor that requires a String value.
     *
     * @param value A String.
     */
    public DecimalLiteral(String value) {
        this(Optional.empty(), value);
    }

    /**
     * Constructor that requires a {@link NodeLocation} and a String value.
     *
     * @param location A {@link NodeLocation}.
     * @param value    A String.
     */
    public DecimalLiteral(NodeLocation location, String value) {
        this(Optional.of(location), value);
    }

    private DecimalLiteral(Optional<NodeLocation> location, String value) {
        super(location);
        this.value = requireNonNull(value, "value is null");
    }

    /**
     * Get the {@link #value} of this DecimalLiteral.
     *
     * @return A String.
     */
    public String getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitDecimalLiteral(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DecimalLiteral that = (DecimalLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
