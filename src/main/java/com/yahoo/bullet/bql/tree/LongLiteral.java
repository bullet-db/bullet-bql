/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/LongLiteral.java
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.bql.parser.ParsingException;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LongLiteral extends Literal {
    private final long value;

    /**
     * Constructor that requires a String value.
     *
     * @param value A String.
     */
    public LongLiteral(String value) {
        this(Optional.empty(), value);
    }

    /**
     * Constructor that requires a {@link NodeLocation} and a String value.
     *
     * @param location A {@link NodeLocation}.
     * @param value    A String value.
     */
    public LongLiteral(NodeLocation location, String value) {
        this(Optional.of(location), value);
    }

    private LongLiteral(Optional<NodeLocation> location, String value) {
        super(location);
        requireNonNull(value, "value is null");
        try {
            this.value = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new ParsingException("Invalid numeric literal: " + value);
        }
    }

    /**
     * Get the {@link #value} of this LongLiteral.
     *
     * @return A long.
     */
    public long getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitLongLiteral(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LongLiteral that = (LongLiteral) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }
}
