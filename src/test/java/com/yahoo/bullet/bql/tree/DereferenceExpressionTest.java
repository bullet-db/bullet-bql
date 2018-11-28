/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DereferenceExpressionTest {
    private Identifier base;
    private Identifier field;
    private Identifier subField;
    private DereferenceExpression dereference;

    @BeforeClass
    public void setUp() {
        base = identifier("aaa");
        field = identifier("bbb");
        subField = identifier("ccc");
        dereference = new DereferenceExpression(base, field, subField);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "base is null")
    public void testConstructorNullBase() {
        new DereferenceExpression(null, field, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "fieldName is null")
    public void testConstructorNullField() {
        new DereferenceExpression(base, null, null);
    }

    @Test
    public void testGetChildren() {
        assertEquals(dereference.getChildren(), emptyList());
    }

    @Test
    public void testEquals() {
        DereferenceExpression dereferenceCopy = dereference;
        assertTrue(dereference.equals(dereferenceCopy));
        assertFalse(dereference.equals(null));
        assertFalse(dereference.equals(base));

        DereferenceExpression sameDereference = new DereferenceExpression(identifier("aaa"), field, subField);
        assertTrue(dereference.equals(sameDereference));

        DereferenceExpression dereferenceDiffBase = new DereferenceExpression(identifier("bbb"), field, subField);
        assertFalse(dereference.equals(dereferenceDiffBase));

        DereferenceExpression dereferenceDiffField = new DereferenceExpression(base, identifier("aaa"), subField);
        assertFalse(dereference.equals(dereferenceDiffField));

        DereferenceExpression dereferenceDiffSubField = new DereferenceExpression(base, field, identifier("aaa"));
        assertFalse(dereference.equals(dereferenceDiffSubField));
    }
}
