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

public class ParensExpression extends Expression {
    private final Expression value;

    /**
     * Constructor that requires an {@link Expression} value.
     *
     * @param value An {@link Expression}.
     */
    public ParensExpression(Expression value) {
        this(Optional.empty(), value);
    }

    /**
     * Constructor that requires a {@link NodeLocation} and an {@link Expression} value.
     *
     * @param location A {@link NodeLocation}.
     * @param value    A {@link Expression}.
     */
    public ParensExpression(NodeLocation location, Expression value) {
        this(Optional.of(location), value);
    }

    private ParensExpression(Optional<NodeLocation> location, Expression value) {
        super(location);
        checkArgument(value != null, "value is null");
        this.value = value;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitParensExpression(this, context);
    }

    /**
     * Get the {@link #value} of this ParensExpression.
     *
     * @return An {@link Expression}.
     */
    public Expression getValue() {
        return value;
    }

    @Override
    public SelectItem.Type getType(Class<SelectItem.Type> clazz) {
        return SelectItem.Type.COMPUTATION;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(value);
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
        ParensExpression that = (ParensExpression) obj;
        return Objects.equals(value, that.value);
    }
}
