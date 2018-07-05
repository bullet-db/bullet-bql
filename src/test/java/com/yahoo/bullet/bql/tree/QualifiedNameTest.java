/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class QualifiedNameTest {
    private QualifiedName qualifiedName;

    @BeforeClass
    public void setUp() {
        qualifiedName = QualifiedName.of("aaa", "bbb");
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "name is null")
    public void testConstructorNullName() {
        QualifiedName.of((String) null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "originalParts is empty")
    public void testConstructorEmptyOriginalParts() {
        QualifiedName.of(emptyList());
    }

    @Test
    public void testGetOriginalParts() {
        List<String> originalParts = asList("aaa", "bbb");
        assertEquals(qualifiedName.getOriginalParts(), originalParts);
    }

    @Test
    public void testGetPrefix() {
        assertEquals(qualifiedName.getPrefix(), Optional.of(QualifiedName.of("aaa")));

        QualifiedName qualifiedNameOnePart = QualifiedName.of("aaa");
        assertEquals(qualifiedNameOnePart.getPrefix(), Optional.empty());
    }

    @Test
    public void testGetSuffix() {
        assertEquals(qualifiedName.getSuffix(), "bbb");
    }

    @Test
    public void testEquals() {
        QualifiedName copy = qualifiedName;
        assertTrue(qualifiedName.equals(copy));
        assertFalse(qualifiedName.equals(null));
        assertFalse(qualifiedName.equals(identifier("aaa")));
    }

    @Test
    public void testHashCode() {
        QualifiedName sameQualifiedName = QualifiedName.of("aaa", "bbb");
        assertEquals(qualifiedName.hashCode(), sameQualifiedName.hashCode());
    }
}
