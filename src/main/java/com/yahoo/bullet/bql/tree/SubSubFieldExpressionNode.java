package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;

import java.util.Objects;

@Getter
public class SubSubFieldExpressionNode extends ExpressionNode {
    private final SubFieldExpressionNode subField;
    private final IdentifierNode subKey;
    // Types ignored for equals() and hashCode()
    private final Type type;

    public SubSubFieldExpressionNode(SubFieldExpressionNode subField, IdentifierNode subKey, Type type, NodeLocation nodeLocation) {
        super(nodeLocation);
        this.subField = subField;
        this.subKey = subKey;
        this.type = type;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitSubSubFieldExpression(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SubSubFieldExpressionNode)) {
            return false;
        }
        SubSubFieldExpressionNode other = (SubSubFieldExpressionNode) obj;
        return Objects.equals(subField, other.subField) &&
               Objects.equals(subKey, other.subKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subField, subKey);
    }
}
