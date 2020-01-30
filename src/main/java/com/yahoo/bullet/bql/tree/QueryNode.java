/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@RequiredArgsConstructor
public class QueryNode extends Node {
    private final SelectNode select;
    private final StreamNode stream;
    private final ExpressionNode where;
    private final GroupByNode groupBy;
    private final ExpressionNode having;
    private final OrderByNode orderBy;
    private final WindowNode window;
    private final String limit;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitQuery(this, context);
    }
}
