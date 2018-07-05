/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/BooleanLiteral.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.Optional;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class BooleanLiteral extends Literal {
    public static final BooleanLiteral TRUE_LITERAL = new BooleanLiteral(Optional.empty(), "true");
    public static final BooleanLiteral FALSE_LITERAL = new BooleanLiteral(Optional.empty(), "false");

    private final boolean value;

    /**
     * Constructor that requires a String value.
     *
     * @param value A String value.
     */
    public BooleanLiteral(String value) {
        this(Optional.empty(), value);
    }

    /**
     * Constructor that requires a {@link NodeLocation} and a String value.
     *
     * @param location A {@link NodeLocation}.
     * @param value    A String value.
     */
    public BooleanLiteral(NodeLocation location, String value) {
        this(Optional.of(location), value);
    }

    private BooleanLiteral(Optional<NodeLocation> location, String value) {
        super(location);
        requireNonNull(value, "value is null");
        Preconditions.checkArgument(value.toLowerCase(ENGLISH).equals("true") || value.toLowerCase(ENGLISH).equals("false"));

        this.value = value.toLowerCase(ENGLISH).equals("true");
    }

    /**
     * Get the {@link #value} of this BooleanLiteral.
     *
     * @return A String value.
     */
    public boolean getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitBooleanLiteral(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BooleanLiteral other = (BooleanLiteral) obj;
        return Objects.equals(this.value, other.value);
    }
}
