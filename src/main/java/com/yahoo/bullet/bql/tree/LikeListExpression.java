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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class LikeListExpression extends Expression {
    private final List<Expression> values;

    /**
     * Constructor that requires a List of {@link Expression} values.
     *
     * @param values A List of {@link Expression}.
     */
    public LikeListExpression(List<Expression> values) {
        this(Optional.empty(), values);
    }

    /**
     * Constructor that requires a {@link NodeLocation} and a List of {@link Expression} values.
     *
     * @param location A {@link NodeLocation}.
     * @param values   A List of {@link Expression}.
     */
    public LikeListExpression(NodeLocation location, List<Expression> values) {
        this(Optional.of(location), values);
    }

    private LikeListExpression(Optional<NodeLocation> location, List<Expression> values) {
        super(location);
        requireNonNull(values, "values is null");
        checkArgument(!values.isEmpty(), "values cannot be empty");
        this.values = ImmutableList.copyOf(values);
    }

    /**
     * Get the {@link #values} of this LikeListExpression.
     *
     * @return A List of {@link Expression}.
     */
    public List<Expression> getValues() {
        return values;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitLikeListExpression(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LikeListExpression that = (LikeListExpression) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }
}
