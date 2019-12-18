package com.yahoo.bullet.bql.tree;

import lombok.Getter;

import java.util.Objects;

import static com.yahoo.bullet.aggregations.grouping.GroupOperation.GroupOperationType;

@Getter
public class GroupOperationNode extends ExpressionNode {
    private final GroupOperationType op;
    private final ExpressionNode expression;

    public GroupOperationNode(String op, ExpressionNode expression) {
        this.op = GroupOperationType.valueOf(op.toUpperCase());
        this.expression = expression;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitGroupOperation(this, context);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GroupOperationNode)) {
            return false;
        }
        GroupOperationNode other = (GroupOperationNode) obj;
        return op == other.op && Objects.equals(expression, other.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, expression);
    }
}
