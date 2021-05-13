/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class QueryNode extends Node {
    private final SelectNode select;
    private final StreamNode stream;
    private final LateralViewNode lateralView;
    private final ExpressionNode where;
    private final GroupByNode groupBy;
    private final ExpressionNode having;
    private final OrderByNode orderBy;
    private final WindowNode window;
    private final String limit;

    public QueryNode(SelectNode select, StreamNode stream, LateralViewNode lateralView, ExpressionNode where, GroupByNode groupBy,
                     ExpressionNode having, OrderByNode orderBy, WindowNode window, String limit, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.select = select;
        this.stream = stream;
        this.lateralView = lateralView;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.window = window;
        this.limit = limit;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitQuery(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof QueryNode)) {
            return false;
        }
        QueryNode other = (QueryNode) obj;
        return Objects.equals(select, other.select) &&
               Objects.equals(stream, other.stream) &&
               Objects.equals(lateralView, other.lateralView) &&
               Objects.equals(where, other.where) &&
               Objects.equals(groupBy, other.groupBy) &&
               Objects.equals(having, other.having) &&
               Objects.equals(orderBy, other.orderBy) &&
               Objects.equals(window, other.window) &&
               Objects.equals(limit, other.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(select, stream, lateralView, where, groupBy, having, orderBy, window, limit);
    }
}
