package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.parsing.expressions.Operation;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;

@Getter
@RequiredArgsConstructor
public class UnaryExpressionNode extends ExpressionNode {
    private final Operation op;
    private final ExpressionNode expression;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitUnaryExpression(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof UnaryExpressionNode)) {
            return false;
        }
        UnaryExpressionNode other = (UnaryExpressionNode) obj;
        return op == other.op && Objects.equals(expression, other.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, expression);
    }
}
