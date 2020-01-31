/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class FieldExpressionNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new FieldExpressionNode(identifier("abc"), 0, identifier("def"), identifier("ghi"), null, null),
                              new FieldExpressionNode(identifier("---"), 0, identifier("def"), identifier("ghi"), null, null),
                              new FieldExpressionNode(identifier("abc"), 1, identifier("def"), identifier("ghi"), null, null),
                              new FieldExpressionNode(identifier("abc"), 0, identifier("---"), identifier("ghi"), null, null),
                              new FieldExpressionNode(identifier("abc"), 0, identifier("def"), identifier("---"), null, null));
    }

    @Test
    public void testEqualsAndHashCodeTypeDoesntMatter() {
        FieldExpressionNode a = new FieldExpressionNode(identifier("abc"), 0, identifier("def"), identifier("ghi"), Type.LISTOFMAP, Type.STRING);
        FieldExpressionNode b = new FieldExpressionNode(identifier("abc"), 0, identifier("def"), identifier("ghi"), Type.BOOLEAN, null);
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }
}
