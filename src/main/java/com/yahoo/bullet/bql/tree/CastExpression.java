package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class CastExpression extends Expression {
    private final Expression expression;
    private final String castType;

    public CastExpression(Expression expression, String castType) {
        this(Optional.empty(), expression, castType);
    }

    public CastExpression(NodeLocation location, Expression expression, String castType) {
        this(Optional.of(location), expression, castType);
    }

    public CastExpression(Optional<NodeLocation> location, Expression expression, String castType) {
        super(location);
        checkArgument(expression != null, "expression is null");
        checkArgument(castType != null, "castType is null");
        this.expression = expression;
        this.castType = castType;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitCastExpression(this, context);
    }

    public Expression getExpression() {
        return expression;
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
        return ImmutableList.of(expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, castType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CastExpression that = (CastExpression) obj;
        return Objects.equals(expression, that.expression) &&
                Objects.equals(castType, that.castType);
    }
}
