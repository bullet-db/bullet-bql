/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/DereferenceExpression.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import com.yahoo.bullet.bql.tree.SelectItem.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class DereferenceExpression extends Expression {
    private final Identifier base;
    private final Identifier field;
    private final Optional<Identifier> subField;

    /**
     * Constructor that requires an {@link Identifier} base, an {@link Identifier} field and an {@link Identifier}
     * subField.
     *
     * @param base     An {@link Identifier}.
     * @param field    An {@link Identifier}.
     * @param subField An {@link Identifier}.
     */
    public DereferenceExpression(Identifier base, Identifier field, Identifier subField) {
        this(Optional.empty(), base, field, Optional.ofNullable(subField));
    }

    /**
     * Constructor that requires a {@link NodeLocation}, an {@link Identifier} base, an {@link Identifier} field, and
     * an {@link Identifier} subField.
     *
     * @param location A {@link NodeLocation}.
     * @param base     An {@link Identifier}.
     * @param field    An {@link Identifier}.
     * @param subField An {@link Identifier}.
     */
    public DereferenceExpression(NodeLocation location, Identifier base, Identifier field, Identifier subField) {
        this(Optional.of(location), base, field, Optional.ofNullable(subField));
    }

    private DereferenceExpression(Optional<NodeLocation> location, Identifier base, Identifier field,
                                  Optional<Identifier> subField) {
        super(location);
        checkArgument(base != null, "base is null");
        checkArgument(field != null, "fieldName is null");
        this.base = base;
        this.field = field;
        this.subField = subField;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitDereferenceExpression(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }

    /**
     * Get the {@link #base} of this DereferenceExpression.
     *
     * @return An {@link Identifier}.
     */
    public Identifier getBase() {
        return base;
    }

    /**
     * Get the {@link #field} of this DereferenceExpression.
     *
     * @return An {@link Identifier}.
     */
    public Identifier getField() {
        return field;
    }

    /**
     * Get the {@link #subField} of this DereferenceExpression.
     *
     * @return An {@link Optional} {@link Identifier}.
     */
    public Optional<Identifier> getSubField() {
        return subField;
    }

    @Override
    public Type getType(Class<SelectItem.Type> clazz) {
        return Type.SUB_COLUMN;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DereferenceExpression that = (DereferenceExpression) o;
        return Objects.equals(base, that.base) && Objects.equals(field, that.field) &&
               Objects.equals(subField, that.subField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, field, subField);
    }
}
