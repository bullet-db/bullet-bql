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

import com.google.common.collect.ImmutableList;
import com.yahoo.bullet.bql.tree.SelectItem.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

public class Identifier extends Expression {
    private static final Pattern NAME_PATTERN = Pattern.compile("[a-zA-Z_]([a-zA-Z0-9_:@])*");

    private final String value;
    private final boolean delimited;

    /**
     * Constructor that requires a {@link NodeLocation}, a String value and a boolean delimited.
     *
     * @param location  A {@link NodeLocation}.
     * @param value     A String.
     * @param delimited A boolean.
     */
    public Identifier(NodeLocation location, String value, boolean delimited) {
        this(Optional.of(location), value, delimited);
    }

    /**
     * Constructor that requires a String value and a boolean delimited.
     *
     * @param value     A String.
     * @param delimited A boolean.
     */
    public Identifier(String value, boolean delimited) {
        this(Optional.empty(), value, delimited);
    }

    /**
     * Constructor that requires a String.
     *
     * @param value A String.
     */
    public Identifier(String value) {
        this(Optional.empty(), value, !NAME_PATTERN.matcher(value).matches());
    }

    private Identifier(Optional<NodeLocation> location, String value, boolean delimited) {
        super(location);
        this.value = value;
        this.delimited = delimited;

        checkArgument(delimited || NAME_PATTERN.matcher(value).matches(), "value contains illegal characters: %s", value);
    }

    /**
     * Get the {@link #value} of this Identifier.
     *
     * @return A String.
     */
    public String getValue() {
        return value;
    }

    /**
     * Get the {@link #delimited} of this Identifier.
     *
     * @return A boolean.
     */
    public boolean isDelimited() {
        return delimited;
    }

    @Override
    public Type getType(Class<SelectItem.Type> clazz) {
        return Type.COLUMN;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitIdentifier(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Identifier that = (Identifier) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
