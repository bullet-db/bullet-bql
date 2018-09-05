/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/LikePredicate.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LikePredicate extends Expression {
    private final Expression value;
    private final Expression patterns;
    private final Optional<Expression> escape;

    /**
     * Constructor that requires an {@link Expression} value, an {@link Expression} patterns and an Optional of {@link Expression} escape.
     *
     * @param value    An {@link Expression}.
     * @param patterns An {@link Expression}. Currently we use {@link ValueListExpression}.
     * @param escape   An Optional of {@link Expression}.
     */
    public LikePredicate(Expression value, Expression patterns, Optional<Expression> escape) {
        this(Optional.empty(), value, patterns, escape);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, an {@link Expression} value, an {@link Expression} patterns.
     *
     * @param location A {@link NodeLocation}.
     * @param value    An {@link Expression}.
     * @param patterns An {@link Expression}. Currently we use {@link ValueListExpression}.
     */
    public LikePredicate(NodeLocation location, Expression value, Expression patterns) {
        this(Optional.of(location), value, patterns, Optional.empty());
    }

    /**
     * Constructor that requires an {@link Expression} value, an {@link Expression} patterns.
     *
     * @param value    An {@link Expression}.
     * @param patterns An {@link Expression}. Currently we use {@link ValueListExpression}.
     */
    public LikePredicate(Expression value, Expression patterns) {
        this(Optional.empty(), value, patterns, Optional.empty());
    }

    private LikePredicate(Optional<NodeLocation> location, Expression value, Expression patterns, Optional<Expression> escape) {
        super(location);
        requireNonNull(value, "value is null");
        requireNonNull(patterns, "pattern is null");
        requireNonNull(escape, "escape is null");

        this.value = value;
        this.patterns = patterns;
        this.escape = escape;
    }

    /**
     * Get the {@link #value} of this LikePredicate.
     *
     * @return An {@link Expression}.
     */
    public Expression getValue() {
        return value;
    }

    /**
     * Get the {@link #patterns} of this LikePredicate.
     *
     * @return An {@link Expression}.
     */
    public Expression getPatterns() {
        return patterns;
    }

    /**
     * Get the {@link #escape} of this LikePredicate.
     *
     * @return An Optional of {@link Expression}.
     */
    public Optional<Expression> getEscape() {
        return escape;
    }

    /**
     * Get the List of {@link Expression} inside the {@link ValueListExpression}.
     *
     * @return A List of {@link Expression}.
     */
    public List<Expression> getLikeList() {
        return ((ValueListExpression) patterns).getValues();
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitLikePredicate(this, context);
    }

    @Override
    public List<Node> getChildren() {
        ImmutableList.Builder<Node> result = ImmutableList.<Node>builder()
                .add(value)
                .add(patterns);

        escape.ifPresent(result::add);
        return result.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LikePredicate that = (LikePredicate) o;

        return Objects.equals(value, that.value) &&
                Objects.equals(patterns, that.patterns) &&
                Objects.equals(escape, that.escape);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, patterns, escape);
    }
}
