/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/ArithmeticUnaryExpression.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ArithmeticUnaryExpression extends Expression {
    // The Sign that decides positive or negative.
    public enum Sign {
        PLUS,
        MINUS
    }

    private final Expression value;
    private final Sign sign;

    /**
     * Constructor that requires a Sign and an {@link Expression}.
     *
     * @param sign  A Sign that determines positive or negative.
     * @param value An {@link Expression}.
     */
    public ArithmeticUnaryExpression(Sign sign, Expression value) {
        this(Optional.empty(), sign, value);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a Sign and an {@link Expression}.
     *
     * @param location A {@link NodeLocation}.
     * @param sign     A Sign that determines positive or negative.
     * @param value    An {@link Expression}.
     */
    public ArithmeticUnaryExpression(NodeLocation location, Sign sign, Expression value) {
        this(Optional.of(location), sign, value);
    }

    private ArithmeticUnaryExpression(Optional<NodeLocation> location, Sign sign, Expression value) {
        super(location);
        requireNonNull(value, "value is null");
        requireNonNull(sign, "sign is null");

        this.value = value;
        this.sign = sign;
    }

    /**
     * Construct a ArithmeticUnaryExpression that has a PLUS Sign.
     *
     * @param location A {@link NodeLocation}.
     * @param value    An {@link Expression}.
     * @return A ArithmeticUnaryExpression.
     */
    public static ArithmeticUnaryExpression positive(NodeLocation location, Expression value) {
        return new ArithmeticUnaryExpression(Optional.of(location), Sign.PLUS, value);
    }

    /**
     * Construct a ArithmeticUnaryExpression that has a MINUS Sign.
     *
     * @param location A {@link NodeLocation}.
     * @param value    An {@link Expression}.
     * @return A ArithmeticUnaryExpression.
     */
    public static ArithmeticUnaryExpression negative(NodeLocation location, Expression value) {
        return new ArithmeticUnaryExpression(Optional.of(location), Sign.MINUS, value);
    }

    /**
     * Construct a ArithmeticUnaryExpression that has a PLUS Sign.
     *
     * @param value An {@link Expression}.
     * @return A ArithmeticUnaryExpression.
     */
    public static ArithmeticUnaryExpression positive(Expression value) {
        return new ArithmeticUnaryExpression(Optional.empty(), Sign.PLUS, value);
    }

    /**
     * Construct a ArithmeticUnaryExpression that has a MINUS Sign.
     *
     * @param value An {@link Expression}.
     * @return A ArithmeticUnaryExpression.
     */
    public static ArithmeticUnaryExpression negative(Expression value) {
        return new ArithmeticUnaryExpression(Optional.empty(), Sign.MINUS, value);
    }

    /**
     * Get the {@link #value} of this ArithmeticUnaryExpression.
     *
     * @return An {@link Expression}.
     */
    public Expression getValue() {
        return value;
    }

    /**
     * Get the {@link #sign} of this ArithmeticUnaryExpression.
     *
     * @return A Sign that determines positive or negative.
     */
    public Sign getSign() {
        return sign;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitArithmeticUnary(this, context);
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

        ArithmeticUnaryExpression that = (ArithmeticUnaryExpression) o;
        return Objects.equals(value, that.value) &&
                (sign == that.sign);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, sign);
    }
}
