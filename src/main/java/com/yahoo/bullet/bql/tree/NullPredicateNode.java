package com.yahoo.bullet.bql.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;

@Getter
@RequiredArgsConstructor
public class NullPredicateNode extends ExpressionNode {
    private final ExpressionNode expression;
    private final boolean not;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitNullPredicate(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof NullPredicateNode)) {
            return false;
        }
        NullPredicateNode other = (NullPredicateNode) obj;
        return Objects.equals(expression, other.expression) && not == other.not;
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, not);
    }
}
