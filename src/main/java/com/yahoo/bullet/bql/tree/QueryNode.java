package com.yahoo.bullet.bql.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Objects;

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

    @Override
    public String toString() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QueryNode)) {
            return false;
        }
        QueryNode other = (QueryNode) obj;
        return Objects.equals(select, other.select) &&
               Objects.equals(stream, other.stream) &&
               Objects.equals(where, other.where) &&
               Objects.equals(groupBy, other.groupBy) &&
               Objects.equals(having, other.having) &&
               Objects.equals(orderBy, other.orderBy) &&
               Objects.equals(window, other.window) &&
               Objects.equals(limit, other.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(select, stream, where, groupBy, having, orderBy, window, limit);
    }
}
