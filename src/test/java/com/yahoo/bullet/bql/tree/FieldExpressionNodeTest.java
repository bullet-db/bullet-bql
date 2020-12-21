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

public class FieldExpressionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new FieldExpressionNode(identifier("abc"), null, null),
                                        new FieldExpressionNode(identifier("---"), null, null));
    }

    @Test
    public void testEqualsAndHashCodeTypeDoesntMatter() {
        FieldExpressionNode a = new FieldExpressionNode(identifier("abc"), Type.STRING_MAP_LIST, null);
        FieldExpressionNode b = new FieldExpressionNode(identifier("abc"), Type.BOOLEAN, null);
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }
}
