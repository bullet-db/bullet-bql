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

import com.yahoo.bullet.bql.util.ExpressionFormatter;

public abstract class ExpressionNode extends Node {
    protected ExpressionNode(NodeLocation location) {
        super(location);
    }

    @Override
    protected <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitExpression(this, context);
    }

    /**
     * Convert this ExpressionNode to String without BQL format.
     *
     * @return A String.
     */
    public String getName() {
        return ExpressionFormatter.format(this, false);
    }

    @Override
    public String toString() {
        return getName();
    }
}
