package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;

@Getter
@RequiredArgsConstructor
public class FieldExpressionNode extends ExpressionNode {
    private final IdentifierNode field;
    private final Integer index;
    private final IdentifierNode key;
    private final IdentifierNode subKey;
    private final Type type;
    private final Type primitiveType;

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitFieldExpression(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof FieldExpressionNode)) {
            return false;
        }
        FieldExpressionNode other = (FieldExpressionNode) obj;
        return Objects.equals(field, other.field) &&
               Objects.equals(index, other.index) &&
               Objects.equals(key, other.key) &&
               Objects.equals(subKey, other.subKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, index, key, subKey);
    }
}
