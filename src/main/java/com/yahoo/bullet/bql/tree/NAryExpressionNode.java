package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.parsing.expressions.Operation;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Objects;

@Getter
@RequiredArgsConstructor
public class NAryExpressionNode extends ExpressionNode {
    private final Operation op;
    private final List<ExpressionNode> expressions;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitNAryExpression(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof NAryExpressionNode)) {
            return false;
        }
        NAryExpressionNode other = (NAryExpressionNode) obj;
        return op == other.op && Objects.equals(expressions, other.expressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, expressions);
    }
}
