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

public class CastExpression extends Expression {
    private final Expression expression;
    private final String castType;

    /**
     * Constructor that requires an {@link Expression} and a {@link String} castType.
     *
     * @param expression An {@link Expression}.
     * @param castType A {@link String}.
     */
    public CastExpression(Expression expression, String castType) {
        this(Optional.empty(), expression, castType);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, an {@link Expression}, and a {@link String} castType.
     *
     * @param location A {@link NodeLocation}.
     * @param expression An {@link Expression}.
     * @param castType A {@link String}.
     */
    public CastExpression(NodeLocation location, Expression expression, String castType) {
        this(Optional.of(location), expression, castType);
    }

    private CastExpression(Optional<NodeLocation> location, Expression expression, String castType) {
        super(location);
        checkArgument(expression != null, "expression is null");
        checkArgument(castType != null, "castType is null");
        this.expression = expression;
        this.castType = castType;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitCastExpression(this, context);
    }

    /**
     * Get the {@link #expression} of this CastExpression.
     *
     * @return An {@link Expression}.
     */
    public Expression getExpression() {
        return expression;
    }

    /**
     * Get the {@link #castType} of this CastExpression.
     *
     * @return A {@link String}.
     */
    public String getCastType() {
        return castType;
    }

    @Override
    public SelectItem.Type getType(Class<SelectItem.Type> clazz) {
        return SelectItem.Type.COMPUTATION;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, castType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CastExpression that = (CastExpression) obj;
        return Objects.equals(expression, that.expression) &&
                Objects.equals(castType, that.castType);
    }
}
