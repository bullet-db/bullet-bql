/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/tree/QualifiedName.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.asList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class QualifiedName {
    private final List<String> parts;
    private final List<String> originalParts;

    /**
     * Construct a QualifiedName from a String first and a List of String rest.
     *
     * @param first A String.
     * @param rest  A List of String.
     * @return A QualifiedName.
     */
    public static QualifiedName of(String first, String... rest) {
        requireNonNull(first, "first is null");
        return of(ImmutableList.copyOf(asList(first, rest)));
    }

    /**
     * Construct a QualifiedName from a String name.
     *
     * @param name A String.
     * @return A QualifiedName.
     */
    public static QualifiedName of(String name) {
        requireNonNull(name, "name is null");
        return of(ImmutableList.of(name));
    }

    /**
     * Construct a QualifiedName from an Iterable of String.
     *
     * @param originalParts An Iterable of String.
     * @return A QualifiedName.
     */
    public static QualifiedName of(Iterable<String> originalParts) {
        requireNonNull(originalParts, "originalParts is null");
        checkArgument(!isEmpty(originalParts), "originalParts is empty");
        List<String> parts = ImmutableList.copyOf(transform(originalParts, part -> part.toLowerCase(ENGLISH)));

        return new QualifiedName(ImmutableList.copyOf(originalParts), parts);
    }

    private QualifiedName(List<String> originalParts, List<String> parts) {
        this.originalParts = originalParts;
        this.parts = parts;
    }

    /**
     * Get the {@link #parts} of this QualifiedName.
     *
     * @return A List of String.
     */
    public List<String> getParts() {
        return parts;
    }

    /**
     * Get the {@link #originalParts} of this QualifiedName.
     *
     * @return A List of String.
     */
    public List<String> getOriginalParts() {
        return originalParts;
    }

    @Override
    public String toString() {
        return Joiner.on('.').join(parts);
    }

    /**
     * For an identifier of the form "a.b.c.d", returns "a.b.c".
     * For an identifier of the form "a", returns absent
     *
     * @return An Optional of QualifiedName.
     */
    public Optional<QualifiedName> getPrefix() {
        if (parts.size() == 1) {
            return Optional.empty();
        }

        List<String> subList = parts.subList(0, parts.size() - 1);
        return Optional.of(new QualifiedName(subList, subList));
    }

    /**
     * Check if this QualifiedName has another QualifiedName as its suffix.
     *
     * @param suffix Another QualifiedName.
     * @return true if this has another QualifiedName as suffix. Otherwise, return false.
     */
    public boolean hasSuffix(QualifiedName suffix) {
        if (parts.size() < suffix.getParts().size()) {
            return false;
        }

        int start = parts.size() - suffix.getParts().size();

        return parts.subList(start, parts.size()).equals(suffix.getParts());
    }

    /**
     * Get the suffix of this QualifiedName.
     *
     * @return A String.
     */
    public String getSuffix() {
        return Iterables.getLast(parts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return parts.equals(((QualifiedName) o).parts);
    }

    @Override
    public int hashCode() {
        return parts.hashCode();
    }
}
