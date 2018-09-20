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

public class BinaryExpression extends Expression {
    private final Expression left;
    private final Expression right;
    private final String op;

    /**
     * Constructor that requires an {@link Expression} left, an {@link Expression} right, and aa {@link String} op.
     *
     * @param left An {@link Expression}.
     * @param right An {@link Expression}.
     * @param op A {@link String}.
     */
    public BinaryExpression(Expression left, Expression right, String op) {
        this(Optional.empty(), left, right, op);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, an {@link Expression} left, an {@link Expression} right, and aa {@link String} op.
     *
     * @param location A {@link NodeLocation}.
     * @param left An {@link Expression}.
     * @param right An {@link Expression}.
     * @param op A {@link String}.
     */
    public BinaryExpression(NodeLocation location, Expression left, Expression right, String op) {
        this(Optional.of(location), left, right, op);
    }

    private BinaryExpression(Optional<NodeLocation> location, Expression left, Expression right, String op) {
        super(location);
        checkArgument(left != null, "left is null");
        checkArgument(right != null, "right is null");
        checkArgument(op != null, "op is null");
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryExpression(this, context);
    }

    /**
     * Get the {@link #left} of this BinaryExpression.
     *
     * @return An {@link Expression}.
     */
    public Expression getLeft() {
        return left;
    }

    /**
     * Get the {@link #right} of this BinaryExpression.
     *
     * @return An {@link Expression}.
     */
    public Expression getRight() {
        return right;
    }

    /**
     * Get the {@link #op} of this BinaryExpression.
     *
     * @return A {@link String}.
     */
    public String getOp() {
        return op;
    }

    @Override
    public SelectItem.Type getType(Class<SelectItem.Type> clazz) {
        return SelectItem.Type.COMPUTATION;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(left, right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, op);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BinaryExpression that = (BinaryExpression) obj;
        return Objects.equals(left, that.left) &&
                Objects.equals(right, that.right) &&
                Objects.equals(op, that.op);
    }
}
