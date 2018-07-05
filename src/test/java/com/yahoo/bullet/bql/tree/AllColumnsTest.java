/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.bql.parser.ParsingException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AllColumnsTest {
    private AllColumns allColumns;
    private QualifiedName prefix;
    private Identifier alias;

    @BeforeClass
    public void setUp() {
        prefix = QualifiedName.of("aaa");
        alias = identifier("bbb");
        allColumns = new AllColumns(prefix, Optional.of(alias));
    }

    @Test
    public void testGetPrefix() {
        assertEquals(allColumns.getPrefix(), Optional.of(prefix));
    }

    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = "\\Qline 1:1: cannot getValue from *\\E.*")
    public void testGetValue() {
        AllColumns allColumnsNoPrefix = new AllColumns();
        allColumnsNoPrefix.getValue();
    }

    @Test
    public void testGetChildren() {
        List<Node> expected = emptyList();
        assertEquals(allColumns.getChildren(), expected);
    }

    @Test
    public void testEquals() {
        AllColumns copy = allColumns;
        assertTrue(allColumns.equals(copy));
        assertFalse(allColumns.equals(null));
        assertFalse(allColumns.equals(prefix));

        QualifiedName diffPrefix = QualifiedName.of("ccc");
        AllColumns allColumnsDiffPrefix = new AllColumns(diffPrefix, Optional.of(alias));
        assertFalse(allColumns.equals(allColumnsDiffPrefix));

        Identifier diffAlias = identifier("ddd");
        AllColumns allColumnsDiffAlias = new AllColumns(prefix, Optional.of(diffAlias));
        assertFalse(allColumns.equals(allColumnsDiffAlias));
    }

    @Test
    public void testHashCode() {
        AllColumns same = new AllColumns(QualifiedName.of("aaa"), Optional.of(identifier("bbb")));
        assertEquals(allColumns.hashCode(), same.hashCode());
    }
}
