package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class LeafExpression extends Expression {
    private final Expression value;

    public LeafExpression(Expression value) {
        this(Optional.empty(), value);
    }

    public LeafExpression(NodeLocation location, Expression value) {
        this(Optional.of(location), value);
    }

    public LeafExpression(Optional<NodeLocation> location, Expression value) {
        super(location);
        checkArgument(value != null, "value is null");
        this.value = value;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitLeafExpression(this, context);
    }

    public Expression getValue() {
        return value;
    }

    @Override
    public SelectItem.Type getType(Class<SelectItem.Type> clazz) {
        return SelectItem.Type.COMPUTATION;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of(value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LeafExpression that = (LeafExpression) obj;
        return Objects.equals(value, that.value);
    }
}
