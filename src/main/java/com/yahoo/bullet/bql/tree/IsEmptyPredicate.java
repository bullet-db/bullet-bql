/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IsEmptyPredicate extends Expression {
    private final Expression value;

    /**
     * Constructor that requires an {@link Expression}.
     *
     * @param value An {@link Expression}.
     */
    public IsEmptyPredicate(Expression value) {
        this(Optional.empty(), value);
    }

    /**
     * Constructor that requires a {@link NodeLocation} and an {@link Expression}.
     *
     * @param location A {@link NodeLocation}.
     * @param value    An {@link Expression}.
     */
    public IsEmptyPredicate(NodeLocation location, Expression value) {
        this(Optional.of(location), value);
    }

    private IsEmptyPredicate(Optional<NodeLocation> location, Expression value) {
        super(location);
        requireNonNull(value, "value is null");
        this.value = value;
    }

    /**
     * Get the {@link #value} of this IsEmptyPredicate.
     *
     * @return An {@link Expression}.
     */
    public Expression getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitIsEmptyPredicate(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IsEmptyPredicate that = (IsEmptyPredicate) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
