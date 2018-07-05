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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class DereferenceExpression extends Expression {
    private final Expression base;
    private final Identifier field;

    /**
     * Constructor that requires an {@link Expression} base and an {@link Identifier} field.
     *
     * @param base  An {@link Expression}.
     * @param field An {@link Identifier}.
     */
    public DereferenceExpression(Expression base, Identifier field) {
        this(Optional.empty(), base, field);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, an {@link Expression} base and an {@link Identifier} field.
     *
     * @param location A {@link NodeLocation}.
     * @param base     An {@link Expression}.
     * @param field    An {@link Identifier}.
     */
    public DereferenceExpression(NodeLocation location, Expression base, Identifier field) {
        this(Optional.of(location), base, field);
    }

    private DereferenceExpression(Optional<NodeLocation> location, Expression base, Identifier field) {
        super(location);
        checkArgument(base != null, "base is null");
        checkArgument(field != null, "fieldName is null");
        this.base = base;
        this.field = field;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitDereferenceExpression(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of(base);
    }

    /**
     * Get the {@link #base} of this DereferenceExpression.
     *
     * @return An {@link Expression}.
     */
    public Expression getBase() {
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
     * If this DereferenceExpression looks like a QualifiedName, return QualifiedName. Otherwise return null.
     *
     * @param expression An DereferenceExpression.
     * @return An {@link QualifiedName}.
     */
    public static QualifiedName getQualifiedName(DereferenceExpression expression) {
        List<String> parts = tryParseParts(expression.base, expression.field.getValue().toLowerCase(Locale.ENGLISH));
        return parts == null ? null : QualifiedName.of(parts);
    }

    /**
     * Convert a {@link QualifiedName} to a DereferenceExpression or a {@link Identifier}.
     *
     * @param name A {@link QualifiedName}.
     * @return A DereferenceExpression or A {@link Identifier}.
     */
    public static Expression from(QualifiedName name) {
        Expression result = null;

        for (String part : name.getParts()) {
            if (result == null) {
                result = new Identifier(part);
            } else {
                result = new DereferenceExpression(result, new Identifier(part));
            }
        }

        return result;
    }

    private static List<String> tryParseParts(Expression base, String fieldName) {
        if (base instanceof Identifier) {
            return ImmutableList.of(((Identifier) base).getValue(), fieldName);
        } else if (base instanceof DereferenceExpression) {
            QualifiedName baseQualifiedName = getQualifiedName((DereferenceExpression) base);
            if (baseQualifiedName != null) {
                List<String> newList = new ArrayList<>(baseQualifiedName.getParts());
                newList.add(fieldName);
                return newList;
            }
        }
        return null;
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
        return Objects.equals(base, that.base) &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, field);
    }
}
