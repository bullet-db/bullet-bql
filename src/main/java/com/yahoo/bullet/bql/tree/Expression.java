/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/Expression.java
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.bql.tree.SelectItem.Type;
import com.yahoo.bullet.bql.util.ExpressionFormatter;

import java.util.Optional;

public abstract class Expression extends Node {
    /**
     * Constructor that requires a {@link NodeLocation}.
     *
     * @param location A {@link NodeLocation}.
     */
    protected Expression(Optional<NodeLocation> location) {
        super(location);
    }

    /**
     * Accessible for {@link ASTVisitor}, use {@link ASTVisitor#process(Node, Object)} instead.
     */
    @Override
    protected <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitExpression(this, context);
    }

    @Override
    public final String toString() {
        return toString(true);
    }

    /**
     * Convert this Expression to String without BQL format.
     *
     * @return A String.
     */
    public final String toFormatlessString() {
        return toString(false);
    }

    /**
     * Get the {@link Type} of this Expression.
     *
     * @param clazz {@link Type} class.
     * @return The {@link Type}.
     */
    public Type getType(Class<Type> clazz) {
        return Type.NON_SELECT;
    }

    /**
     * Compare with another Expression.
     *
     * @param that Another Expression.
     * @return A positive int if this Expression's {@link NodeLocation} is higher that another's.
     */
    public int compareTo(Expression that) {
        if (!getLocation().isPresent() && !that.getLocation().isPresent()) {
            return 0;
        } else if (!that.getLocation().isPresent()) {
            return 1;
        } else if (!getLocation().isPresent()) {
            return -1;
        } else if (getLocation().get().getLineNumber() != that.getLocation().get().getLineNumber()) {
            return getLocation().get().getLineNumber() - that.getLocation().get().getLineNumber();
        } else {
            return getLocation().get().getColumnNumber() - that.getLocation().get().getColumnNumber();
        }
    }

    private String toString(boolean withFormat) {
        return ExpressionFormatter.formatExpression(this, Optional.empty(), withFormat);
    }
}
