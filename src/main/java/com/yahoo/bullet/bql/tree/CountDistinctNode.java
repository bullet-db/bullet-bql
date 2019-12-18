package com.yahoo.bullet.bql.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Objects;

@Getter
@RequiredArgsConstructor
public class CountDistinctNode extends ExpressionNode {
    private final List<ExpressionNode> expressions;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitCountDistinct(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CountDistinctNode && Objects.equals(expressions, ((CountDistinctNode) obj).expressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expressions);
    }
}