/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.tree.DereferenceExpression.from;
import static com.yahoo.bullet.bql.tree.DereferenceExpression.getQualifiedName;
import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class DereferenceExpressionTest {
    private Expression base;
    private Identifier field;
    private DereferenceExpression dereference;

    @BeforeClass
    public void setUp() {
        base = identifier("aaa");
        field = identifier("bbb");
        dereference = new DereferenceExpression(base, field);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "base is null")
    public void testConstructorNullBase() {
        new DereferenceExpression(null, field);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "fieldName is null")
    public void testConstructorNullField() {
        new DereferenceExpression(base, null);
    }

    @Test
    public void testGetChildren() {
        assertEquals(dereference.getChildren(), singletonList(base));
    }

    @Test
    public void testFrom() {
        QualifiedName qualifiedName = QualifiedName.of("AAA", "BBB");
        Expression expression = from(qualifiedName);
        assertEquals(dereference, expression);
    }

    @Test
    public void testGetQualifiedName() {
        QualifiedName qualifiedName = QualifiedName.of("aaa", "bbb");
        assertEquals(getQualifiedName(dereference), qualifiedName);

        Expression base = new NullLiteral();
        DereferenceExpression dereferenceNullLiteralBase = new DereferenceExpression(base, field);
        assertNull(getQualifiedName(dereferenceNullLiteralBase));

        DereferenceExpression dereferenceDereferenceBase = new DereferenceExpression(dereference, field);
        assertEquals(getQualifiedName(dereferenceDereferenceBase), QualifiedName.of("aaa", "bbb", "bbb"));

        DereferenceExpression dereferenceInvalidDereferenceBase = new DereferenceExpression(dereferenceNullLiteralBase, field);
        assertNull(getQualifiedName(dereferenceInvalidDereferenceBase));
    }

    @Test
    public void testEquals() {
        DereferenceExpression dereferenceCopy = dereference;
        assertTrue(dereference.equals(dereferenceCopy));
        assertFalse(dereference.equals(null));
        assertFalse(dereference.equals(base));

        DereferenceExpression sameDereference = new DereferenceExpression(identifier("aaa"), field);
        assertTrue(dereference.equals(sameDereference));

        DereferenceExpression dereferenceDiffBase = new DereferenceExpression(identifier("bbb"), field);
        assertFalse(dereference.equals(dereferenceDiffBase));

        DereferenceExpression dereferenceDiffField = new DereferenceExpression(base, identifier("aaa"));
        assertFalse(dereference.equals(dereferenceDiffField));
    }
}
