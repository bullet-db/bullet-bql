/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/ComparisonExpression.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import com.yahoo.bullet.parsing.Clause.Operation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ComparisonExpression extends Expression {
    private final Operation operation;
    private final Expression left;
    private final Expression right;
    private final boolean isDistinctFrom;

    /**
     * Constructor that requires a {@link Operation} operation, an {@link Expression} left, an {@link Expression} right and a boolean isDistinctFrom.
     *
     * @param operation      A {@link Operation}.
     * @param left           An {@link Expression} left.
     * @param right          An {@link Expression} right.
     * @param isDistinctFrom A boolean.
     */
    public ComparisonExpression(Operation operation, Expression left, Expression right, boolean isDistinctFrom) {
        this(Optional.empty(), operation, left, right, isDistinctFrom);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a {@link Operation} operation, an {@link Expression} left, an {@link Expression} right and a boolean isDistinctFrom.
     *
     * @param location       A {@link NodeLocation}.
     * @param operation      A {@link Operation}.
     * @param left           An {@link Expression} left.
     * @param right          An {@link Expression} right.
     * @param isDistinctFrom A boolean.
     */
    public ComparisonExpression(NodeLocation location, Operation operation, Expression left, Expression right, boolean isDistinctFrom) {
        this(Optional.of(location), operation, left, right, isDistinctFrom);
    }

    private ComparisonExpression(Optional<NodeLocation> location, Operation operation, Expression left, Expression right, boolean isDistinctFrom) {
        super(location);
        requireNonNull(operation, "operation is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.operation = operation;
        this.left = left;
        this.right = right;
        this.isDistinctFrom = isDistinctFrom;
    }

    /**
     * Get the {@link #operation} of this ComparisonExpression.
     *
     * @return A {@link Operation}.
     */
    public Operation getOperation() {
        return operation;
    }

    /**
     * Get the {@link #left} of this ComparisonExpression.
     *
     * @return An {@link Expression}.
     */
    public Expression getLeft() {
        return left;
    }

    /**
     * Get the {@link #right} of this ComparisonExpression.
     *
     * @return An {@link Expression}.
     */
    public Expression getRight() {
        return right;
    }

    /**
     * Get the {@link #isDistinctFrom} of this ComparisonExpression.
     *
     * @return A boolean.
     */
    public boolean isDistinctFrom() {
        return isDistinctFrom;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitComparisonExpression(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of(left, right);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ComparisonExpression that = (ComparisonExpression) o;
        return (operation == that.operation) &&
                Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operation, left, right);
    }
}
