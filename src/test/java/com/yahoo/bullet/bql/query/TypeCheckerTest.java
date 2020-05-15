/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.querying.aggregations.grouping.GroupOperation;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TypeCheckerTest {
    @Test
    public void testValidateUnaryTypeNotUnary() {
        // coverage
        Optional<List<BulletError>> errors = TypeChecker.validateUnaryType(null, null, Operation.ADD);
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0).getError(), "This is not a unary operation: +");
        Assert.assertEquals(errors.get().size(), 1);
    }

    @Test
    public void testGetUnaryTypeNotUnary() {
        // coverage
        Assert.assertEquals(TypeChecker.getUnaryType(Operation.ADD), Type.UNKNOWN);
    }

    @Test
    public void testValidateNAryTypeNotNAry() {
        // coverage
        Optional<List<BulletError>> errors = TypeChecker.validateNAryType(null, Collections.emptyList(), Operation.ADD);
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0).getError(), "This is not a supported n-ary operation: +");
        Assert.assertEquals(errors.get().size(), 1);
    }

    @Test
    public void testGetNAryTypeNotNAry() {
        // coverage
        Assert.assertEquals(TypeChecker.getNAryType(null, Operation.ADD), Type.UNKNOWN);
    }

    @Test
    public void testGetAggregateType() {
        // coverage
        Assert.assertEquals(TypeChecker.getAggregateType(null, GroupOperation.GroupOperationType.COUNT), Type.UNKNOWN);
    }

    @Test
    public void testValidateBinaryTypeNotBinary() {
        // coverage
        Optional<List<BulletError>> errors = TypeChecker.validateBinaryType(null, null, null, Operation.NOT);
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().get(0).getError(), "This is not a binary operation: NOT");
        Assert.assertEquals(errors.get().size(), 1);
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

    @Test
    public void testGetBinaryTypeNotBinary() {
        // coverage
        Assert.assertEquals(TypeChecker.getBinaryType(null, null, Operation.NOT), Type.UNKNOWN);
    }
}
