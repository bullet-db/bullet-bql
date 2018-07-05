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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DoubleLiteral extends Literal {
    private final double value;

    /**
     * Constructor that requires a String value.
     *
     * @param value A String.
     */
    public DoubleLiteral(String value) {
        this(Optional.empty(), value);
    }

    /**
     * Constructor that requires a {@link NodeLocation} and a String value.
     *
     * @param location A {@link NodeLocation}.
     * @param value    A String value.
     */
    public DoubleLiteral(NodeLocation location, String value) {
        this(Optional.of(location), value);
    }

    private DoubleLiteral(Optional<NodeLocation> location, String value) {
        super(location);
        requireNonNull(value, "value is null");
        this.value = Double.parseDouble(value);
    }

    /**
     * Get the {@link #value} of this DoubleLiteral.
     *
     * @return A String.
     */
    public double getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitDoubleLiteral(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DoubleLiteral that = (DoubleLiteral) o;

        return Double.compare(that.value, value) == 0;
    }

    @SuppressWarnings("UnaryPlus")
    @Override
    public int hashCode() {
        long temp = value != +0.0d ? Double.doubleToLongBits(value) : 0L;
        return (int) (temp ^ (temp >>> 32));
    }
}
