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
        NodeUtils.testEqualsAndHashCode(() -> new SubFieldExpressionNode(identifier("abc"), 0, identifier("def"), identifier("ghi"), "jkl", null, null),
                                        new SubFieldExpressionNode(identifier("---"), 0, identifier("def"), identifier("ghi"), "jkl", null, null),
                                        new SubFieldExpressionNode(identifier("abc"), 1, identifier("def"), identifier("ghi"), "jkl", null, null),
                                        new SubFieldExpressionNode(identifier("abc"), 0, identifier("---"), identifier("ghi"), "jkl", null, null),
                                        new SubFieldExpressionNode(identifier("abc"), 0, identifier("def"), identifier("---"), "jkl", null, null),
                                        new SubFieldExpressionNode(identifier("abc"), 0, identifier("def"), identifier("ghi"), "---", null, null));
    }

    @Test
    public void testEqualsAndHashCodeTypeDoesntMatter() {
        SubFieldExpressionNode a = new SubFieldExpressionNode(identifier("abc"), 0, identifier("def"), null, null, Type.STRING_MAP_LIST, null);
        SubFieldExpressionNode b = new SubFieldExpressionNode(identifier("abc"), 0, identifier("def"), null, null, Type.BOOLEAN, null);
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
    }
}
