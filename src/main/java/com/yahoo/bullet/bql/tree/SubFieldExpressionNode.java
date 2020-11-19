package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.Objects;

@Getter
public class SubFieldExpressionNode extends ExpressionNode {
    private final ExpressionNode field;
    private final Integer index;
    private final IdentifierNode key;
    // Types ignored for equals() and hashCode()
    private final Type type;

    public SubFieldExpressionNode(ExpressionNode field, Integer index, IdentifierNode key, Type type, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.field = field;
        this.index = index;
        this.key = key;
        this.type = type;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitSubFieldExpression(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SubFieldExpressionNode)) {
            return false;
        }
        SubFieldExpressionNode other = (SubFieldExpressionNode) obj;
        return Objects.equals(field, other.field) &&
               Objects.equals(index, other.index) &&
               Objects.equals(key, other.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, index, key);
    }
}
