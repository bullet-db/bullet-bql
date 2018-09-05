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

public class ContainsPredicate extends Expression {
    private final Expression value;
    private final Expression valueList;
    private final Operation operation;

    /**
     * Constructor that requires a {@link Operation} operation, an {@link Expression} value and an {@link Expression} valueList.
     *
     * @param operation A {@link Operation}.
     * @param value     An {@link Expression}.
     * @param valueList An {@link Expression}. Currently we use {@link ValueListExpression}.
     */
    public ContainsPredicate(Operation operation, Expression value, Expression valueList) {
        this(Optional.empty(), operation, value, valueList);
    }

    /**
     * Constructor that requires a{@link NodeLocation}, a {@link Operation} operation, an {@link Expression} value and an {@link Expression} valueList.
     *
     * @param location  A {@link NodeLocation}.
     * @param operation A {@link Operation}.
     * @param value     An {@link Expression}.
     * @param valueList An {@link Expression}. Currently we use {@link ValueListExpression}.
     */
    public ContainsPredicate(NodeLocation location, Operation operation, Expression value, Expression valueList) {
        this(Optional.of(location), operation, value, valueList);
    }

    private ContainsPredicate(Optional<NodeLocation> location, Operation operation, Expression value, Expression valueList) {
        super(location);
        this.operation = operation;
        this.value = value;
        this.valueList = valueList;
    }

    /**
     * Get the {@link #value} of this ContainsPredicate.
     *
     * @return An {@link Expression}.
     */
    public Expression getValue() {
        return value;
    }

    /**
     * Get the {@link #valueList} of this ContainsPredicate.
     *
     * @return An {@link Expression}.
     */
    public Expression getValueList() {
        return valueList;
    }

    /**
     * Get the List of {@link Expression} inside {@link #valueList}.
     *
     * @return A List of {@link Expression}.
     */
    public List<Expression> getContainsList() {
        return ((ValueListExpression) valueList).getValues();
    }

    /**
     * Get the {@link #operation} of this ContainsPredicate.
     *
     * @return A {@link Operation}.
     */
    public Operation getOperation() {
        return operation;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitContainsPredicate(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of(value, valueList);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ContainsPredicate that = (ContainsPredicate) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(valueList, that.valueList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, valueList);
    }
}
