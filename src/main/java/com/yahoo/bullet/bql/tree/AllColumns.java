/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/AllColumns.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import com.yahoo.bullet.bql.parser.ParsingException;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AllColumns extends SelectItem {
    private final Optional<QualifiedName> prefix;
    private final Optional<Identifier> alias;

    /**
     * The AllColumns constructor.
     */
    public AllColumns() {
        super(Optional.empty());
        prefix = Optional.empty();
        this.alias = Optional.empty();
    }

    /**
     * Constructor that requires a {@link NodeLocation}.
     *
     * @param location The {@link NodeLocation} of the AllColumns.
     */
    public AllColumns(NodeLocation location) {
        super(Optional.of(location));
        prefix = Optional.empty();
        this.alias = Optional.empty();
    }

    /**
     * Constructor that requires a {@link QualifiedName} and an alias.
     *
     * @param prefix A {@link QualifiedName}.
     * @param alias  An Optional of Identifier.
     */
    public AllColumns(QualifiedName prefix, Optional<Identifier> alias) {
        this(Optional.empty(), prefix, alias);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, a {@link QualifiedName} and an of alias.
     *
     * @param location The {@link NodeLocation} of the AllColumns.
     * @param prefix   A {@link QualifiedName}.
     * @param alias    An Optional of Identifier.
     */
    public AllColumns(NodeLocation location, QualifiedName prefix, Optional<Identifier> alias) {
        this(Optional.of(location), prefix, alias);
    }

    private AllColumns(Optional<NodeLocation> location, QualifiedName prefix, Optional<Identifier> alias) {
        super(location);
        requireNonNull(prefix, "prefix is null");
        requireNonNull(alias, "alias is null");
        this.prefix = Optional.of(prefix);
        this.alias = alias;
    }

    /**
     * Get the {@link #prefix} of this AllColumns.
     *
     * @return An Optional of {@link QualifiedName}.
     */
    public Optional<QualifiedName> getPrefix() {
        return prefix;
    }

    /**
     * Get the {@link #alias} of this AllColumns.
     *
     * @return An Optional of Identifier.
     */
    public Optional<Identifier> getAlias() {
        return alias;
    }

    @Override
    public Type getType() {
        if (prefix.isPresent()) {
            return Type.SUB_ALL;
        }

        return Type.ALL;
    }

    @Override
    public Expression getValue() {
        if (prefix.isPresent()) {
            return new Identifier(prefix.get().toString());
        }
        throw new ParsingException("cannot getValue from *");
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitAllColumns(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AllColumns that = (AllColumns) o;
        return Objects.equals(prefix, that.prefix) &&
                Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return prefix.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        prefix.ifPresent(qualifiedName -> sb.append(qualifiedName)
                .append("."));

        sb.append("*");

        alias.ifPresent(identifier -> sb.append(" ")
                .append(identifier));
        return sb.toString();
    }
}
