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

public class SubFieldExpressionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        NodeUtils.testEqualsAndHashCode(() -> new SubFieldExpressionNode(identifier("abc"), 0, identifier("def"), null, null),
                                        new SubFieldExpressionNode(identifier("---"), 0, identifier("def"), null, null),
                                        new SubFieldExpressionNode(identifier("abc"), 1, identifier("def"), null, null),
                                        new SubFieldExpressionNode(identifier("abc"), 0, identifier("---"), null, null));
    }

    @Test
    public void testEqualsAndHashCodeTypeDoesntMatter() {
        SubFieldExpressionNode a = new SubFieldExpressionNode(identifier("abc"), 0, identifier("def"), Type.STRING_MAP_LIST, null);
        SubFieldExpressionNode b = new SubFieldExpressionNode(identifier("abc"), 0, identifier("def"), Type.BOOLEAN, null);
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }
}
