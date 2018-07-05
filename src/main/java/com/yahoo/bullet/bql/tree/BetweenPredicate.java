/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/BetweenPredicate.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BetweenPredicate extends Expression {
    private final Expression value;
    private final Expression min;
    private final Expression max;

    /**
     * Constructor that requires an {@link Expression} value, an {@link Expression} min and an {@link Expression} max.
     *
     * @param value An {@link Expression}.
     * @param min   An {@link Expression}.
     * @param max   An {@link Expression}.
     */
    public BetweenPredicate(Expression value, Expression min, Expression max) {
        this(Optional.empty(), value, min, max);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, an {@link Expression} value, an {@link Expression} min and an {@link Expression} max.
     *
     * @param location a {@link NodeLocation}.
     * @param value    An {@link Expression}.
     * @param min      An {@link Expression}.
     * @param max      An {@link Expression}.
     */
    public BetweenPredicate(NodeLocation location, Expression value, Expression min, Expression max) {
        this(Optional.of(location), value, min, max);
    }

    private BetweenPredicate(Optional<NodeLocation> location, Expression value, Expression min, Expression max) {
        super(location);
        requireNonNull(value, "value is null");
        requireNonNull(min, "min is null");
        requireNonNull(max, "max is null");

        this.value = value;
        this.min = min;
        this.max = max;
    }

    /**
     * Get the {@link #value} of this BetweenPredicate.
     *
     * @return An {@link Expression}.
     */
    public Expression getValue() {
        return value;
    }

    /**
     * Get the {@link #min} of this BetweenPredicate.
     *
     * @return An {@link Expression}.
     */
    public Expression getMin() {
        return min;
    }

    /**
     * Get the {@link #max} of this BetweenPredicate.
     *
     * @return An {@link Expression}.
     */
    public Expression getMax() {
        return max;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitBetweenPredicate(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of(value, min, max);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BetweenPredicate that = (BetweenPredicate) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(min, that.min) &&
                Objects.equals(max, that.max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, min, max);
    }
}
