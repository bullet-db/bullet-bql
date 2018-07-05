/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/SingleColumn.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SingleColumn extends SelectItem {
    private final Optional<Identifier> alias;
    private final Expression expression;

    /**
     * Constructor that requires an {@link Expression}.
     *
     * @param expression An {@link Expression}.
     */
    public SingleColumn(Expression expression) {
        this(Optional.empty(), expression, Optional.empty());
    }

    /**
     * Constructor that requires an {@link Expression} and an {@link Identifier} alias.
     *
     * @param expression An {@link Expression}.
     * @param alias      An {@link Identifier}.
     */
    public SingleColumn(Expression expression, Identifier alias) {
        this(Optional.empty(), expression, Optional.of(alias));
    }

    /**
     * Constructor that requires a {@link NodeLocation}, an {@link Expression} and an {@link Identifier} alias.
     *
     * @param location   A {@link NodeLocation}.
     * @param expression An {@link Expression}.
     * @param alias      An {@link Identifier}.
     */
    public SingleColumn(NodeLocation location, Expression expression, Optional<Identifier> alias) {
        this(Optional.of(location), expression, alias);
    }

    private SingleColumn(Optional<NodeLocation> location, Expression expression, Optional<Identifier> alias) {
        super(location);
        requireNonNull(expression, "expression is null");
        requireNonNull(alias, "alias is null");

        this.expression = expression;
        this.alias = alias;
    }

    /**
     * Get the {@link #alias} of this SingleColumn.
     *
     * @return An Optional of Identifier.
     */
    public Optional<Identifier> getAlias() {
        return alias;
    }

    /**
     * Get the {@link #expression} of this SingleColumn.
     *
     * @return An {@link #expression}.
     */
    public Expression getExpression() {
        return expression;
    }

    @Override
    public Type getType() {
        return expression.getType(Type.class);
    }

    @Override
    public Expression getValue() {
        return expression;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SingleColumn other = (SingleColumn) obj;
        return Objects.equals(this.alias, other.alias) && Objects.equals(this.expression, other.expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, expression);
    }

    @Override
    public String toString() {
        if (alias.isPresent()) {
            return expression.toString() + " " + alias.get();
        }

        return expression.toString();
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitSingleColumn(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of(expression);
    }
}
