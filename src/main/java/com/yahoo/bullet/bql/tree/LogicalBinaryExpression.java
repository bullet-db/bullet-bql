/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/LogicalBinaryExpression.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import com.yahoo.bullet.parsing.Clause.Operation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LogicalBinaryExpression extends Expression {
    private final Operation operation;
    private final Expression left;
    private final Expression right;

    /**
     * Constructor that requires an {@link Operation}, an {@link Expression} left and an {@link Expression} right.
     *
     * @param operation An {@link Operation}.
     * @param left      An {@link Expression}.
     * @param right     An {@link Expression}.
     */
    public LogicalBinaryExpression(Operation operation, Expression left, Expression right) {
        this(Optional.empty(), operation, left, right);
    }

    /**
     * Constructor that requires an {@link Operation}, an {@link Expression} left and an {@link Expression} right.
     *
     * @param location  A {@link NodeLocation}.
     * @param operation An {@link Operation}.
     * @param left      An {@link Expression}.
     * @param right     An {@link Expression}.
     */
    public LogicalBinaryExpression(NodeLocation location, Operation operation, Expression left, Expression right) {
        this(Optional.of(location), operation, left, right);
    }

    private LogicalBinaryExpression(Optional<NodeLocation> location, Operation operation, Expression left, Expression right) {
        super(location);
        requireNonNull(operation, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.operation = operation;
        this.left = left;
        this.right = right;
    }

    /**
     * Get the {@link #operation} of this LogicalBinaryExpression.
     *
     * @return An {@link Operation}.
     */
    public Operation getOperation() {
        return operation;
    }

    /**
     * Get the {@link #left} of this LogicalBinaryExpression.
     *
     * @return An {@link Expression}.
     */
    public Expression getLeft() {
        return left;
    }

    /**
     * Get the {@link #right} of this LogicalBinaryExpression.
     *
     * @return An {@link Expression}.
     */
    public Expression getRight() {
        return right;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalBinaryExpression(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of(left, right);
    }

    /**
     * Construct a LogicalBinaryExpression whose {@link #operation} is {@link Operation#AND}.
     *
     * @param left  An {@link Expression}.
     * @param right An {@link Expression}.
     * @return A LogicalBinaryExpression.
     */
    public static LogicalBinaryExpression and(Expression left, Expression right) {
        return new LogicalBinaryExpression(Optional.empty(), Operation.AND, left, right);
    }

    /**
     * Construct a LogicalBinaryExpression whose {@link #operation} is {@link Operation#OR}.
     *
     * @param left  An {@link Expression}.
     * @param right An {@link Expression}.
     * @return A LogicalBinaryExpression.
     */
    public static LogicalBinaryExpression or(Expression left, Expression right) {
        return new LogicalBinaryExpression(Optional.empty(), Operation.OR, left, right);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LogicalBinaryExpression that = (LogicalBinaryExpression) o;
        return operation == that.operation &&
                Objects.equals(left, that.left) &&
                Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operation, left, right);
    }
}
