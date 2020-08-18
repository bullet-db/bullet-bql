/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class TypeCheckerTest {
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported unary operation: \\+")
    public void testValidateUnaryTypeNotUnary() {
        // coverage
        TypeChecker.validateUnaryType(null, null, Operation.ADD);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported unary operation: \\+")
    public void testGetUnaryTypeNotUnary() {
        // coverage
        Assert.assertEquals(TypeChecker.getUnaryType(Operation.ADD), Type.UNKNOWN);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported n-ary operation: \\+")
    public void testValidateNAryTypeNotNAry() {
        // coverage
        TypeChecker.validateNAryType(null, Collections.emptyList(), Operation.ADD);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported n-ary operation: \\+")
    public void testGetNAryTypeNotNAry() {
        // coverage
        Assert.assertEquals(TypeChecker.getNAryType(null, Operation.ADD), Type.UNKNOWN);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported group operation: COUNT")
    public void testGetAggregateType() {
        // coverage
        TypeChecker.getAggregateType(null, GroupOperation.GroupOperationType.COUNT);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported binary operation: NOT")
    public void testValidateBinaryTypeNotBinary() {
        // coverage
        TypeChecker.validateBinaryType(null, null, null, Operation.NOT);
    }

    @Test
    public void testGetBinaryTypeWidening() {
        // coverage
        Assert.assertEquals(TypeChecker.getBinaryType(Type.DOUBLE, Type.DOUBLE, Operation.ADD), Type.DOUBLE);
        Assert.assertEquals(TypeChecker.getBinaryType(Type.DOUBLE, Type.INTEGER, Operation.ADD), Type.DOUBLE);
        Assert.assertEquals(TypeChecker.getBinaryType(Type.INTEGER, Type.DOUBLE, Operation.ADD), Type.DOUBLE);
        Assert.assertEquals(TypeChecker.getBinaryType(Type.FLOAT, Type.FLOAT, Operation.ADD), Type.FLOAT);
        Assert.assertEquals(TypeChecker.getBinaryType(Type.FLOAT, Type.INTEGER, Operation.ADD), Type.FLOAT);
        Assert.assertEquals(TypeChecker.getBinaryType(Type.INTEGER, Type.FLOAT, Operation.ADD), Type.FLOAT);
        Assert.assertEquals(TypeChecker.getBinaryType(Type.LONG, Type.LONG, Operation.ADD), Type.LONG);
        Assert.assertEquals(TypeChecker.getBinaryType(Type.LONG, Type.INTEGER, Operation.ADD), Type.LONG);
        Assert.assertEquals(TypeChecker.getBinaryType(Type.INTEGER, Type.LONG, Operation.ADD), Type.LONG);
        Assert.assertEquals(TypeChecker.getBinaryType(Type.INTEGER, Type.INTEGER, Operation.ADD), Type.INTEGER);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "This is not a supported binary operation: NOT")
    public void testGetBinaryTypeNotBinary() {
        // coverage
        TypeChecker.getBinaryType(null, null, Operation.NOT);
    }
}
