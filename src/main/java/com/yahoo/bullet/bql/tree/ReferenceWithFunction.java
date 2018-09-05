/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/InPredicate.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import com.yahoo.bullet.parsing.Clause.Operation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class ReferenceWithFunction extends Expression {
    private final Expression value;
    private final Operation operation;

    /**
     * Constructor that requires a {@link Operation} and an {@link Expression} value.
     *
     * @param operation A {@link Operation}.
     * @param value     An {@link Expression}.
     */
    public ReferenceWithFunction(Operation operation, Expression value) {
        this(Optional.empty(), operation, value);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a {@link Operation} and  an {@link Expression} value.
     *
     * @param location  A {@link NodeLocation}.
     * @param operation A {@link Operation}.
     * @param value     An {@link Expression}.
     */
    public ReferenceWithFunction(NodeLocation location, Operation operation, Expression value) {
        this(Optional.of(location), operation, value);
    }

    private ReferenceWithFunction(Optional<NodeLocation> location, Operation operation, Expression value) {
        super(location);
        this.operation = operation;
        this.value = value;
    }

    /**
     * Get the {@link #value} of this ReferenceWithFunction.
     *
     * @return An {@link Expression}.
     */
    public Expression getValue() {
        return value;
    }

    /**
     * Get the {@link #operation} of this ReferenceWithFunction.
     *
     * @return A {@link Operation}.
     */
    public Operation getOperation() {
        return operation;
    }


    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitReferenceWithFunction(this, context);
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

        ReferenceWithFunction that = (ReferenceWithFunction) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
