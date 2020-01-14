package com.yahoo.bullet.bql.tree;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;

@Getter
@RequiredArgsConstructor
public class ParenthesesExpressionNode extends ExpressionNode {
    private final ExpressionNode expression;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitParenthesesExpression(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        return obj instanceof ParenthesesExpressionNode && Objects.equals(expression, ((ParenthesesExpressionNode) obj).expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression);
    }
}
