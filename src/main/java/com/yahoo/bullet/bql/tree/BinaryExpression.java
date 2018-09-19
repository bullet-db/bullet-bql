package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class BinaryExpression extends Expression {
    private final Expression left;
    private final Expression right;
    private final String op;
    private final String castType;

    public BinaryExpression(Expression left, Expression right, String op, String castType) {
        this(Optional.empty(), left, right, op, castType);
    }

    public BinaryExpression(NodeLocation location, Expression left, Expression right, String op, String castType) {
        this(Optional.of(location), left, right, op, castType);
    }

    public BinaryExpression(Optional<NodeLocation> location, Expression left, Expression right, String op, String castType) {
        super(location);
        checkArgument(left != null, "left is null");
        checkArgument(right != null, "right is null");
        checkArgument(op != null, "op is null");
        this.left = left;
        this.right = right;
        this.op = op;
        this.castType = castType;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryExpression(this, context);
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    public String getOp() {
        return op;
    }

    public String getCastType() {
        return castType;
    }

    @Override
    public SelectItem.Type getType(Class<SelectItem.Type> clazz) {
        return SelectItem.Type.COMPUTATION;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(left, right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right, op, castType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BinaryExpression that = (BinaryExpression) obj;
        return Objects.equals(left, that.left) &&
                Objects.equals(right, that.right) &&
                Objects.equals(op, that.op) &&
                Objects.equals(castType, that.castType);
    }
}
