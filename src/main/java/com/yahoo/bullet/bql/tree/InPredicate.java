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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class InPredicate extends Expression {
    private final Expression value;
    private final Expression valueList;

    /**
     * Constructor that requires an {@link Expression} value and an {@link Expression} valueList.
     *
     * @param value     An {@link Expression}.
     * @param valueList An {@link Expression}. Currently we use {@link InListExpression}.
     */
    public InPredicate(Expression value, Expression valueList) {
        this(Optional.empty(), value, valueList);
    }

    /**
     * Constructor that requires an {@link Expression} value and an {@link Expression} valueList.
     *
     * @param location  A {@link NodeLocation}.
     * @param value     An {@link Expression}.
     * @param valueList An {@link Expression}. Currently we use {@link InListExpression}.
     */
    public InPredicate(NodeLocation location, Expression value, Expression valueList) {
        this(Optional.of(location), value, valueList);
    }

    private InPredicate(Optional<NodeLocation> location, Expression value, Expression valueList) {
        super(location);
        this.value = value;
        this.valueList = valueList;
    }

    /**
     * Get the {@link #value} of this InPredicate.
     *
     * @return An {@link Expression}.
     */
    public Expression getValue() {
        return value;
    }

    /**
     * Get the {@link #valueList} of this InPredicate.
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
    public List<Expression> getInList() {
        return ((InListExpression) valueList).getValues();
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitInPredicate(this, context);
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

        InPredicate that = (InPredicate) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(valueList, that.valueList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, valueList);
    }
}
