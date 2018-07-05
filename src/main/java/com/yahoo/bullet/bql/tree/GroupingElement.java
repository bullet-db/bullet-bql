/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/GroupingElement.java
 */
package com.yahoo.bullet.bql.tree;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public abstract class GroupingElement extends Node {
    /**
     * Constructor that requires a {@link NodeLocation}.
     *
     * @param location A {@link NodeLocation}.
     */
    protected GroupingElement(Optional<NodeLocation> location) {
        super(location);
    }

    /**
     * Get a List of Set of {@link Expression} in this GroupingElement.
     *
     * @return A List of Set of {@link Expression}.
     */
    public abstract List<Set<Expression>> enumerateGroupingSets();

    @Override
    protected <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitGroupingElement(this, context);
    }
}
