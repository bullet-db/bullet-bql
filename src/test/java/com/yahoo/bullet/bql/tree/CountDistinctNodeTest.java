package com.yahoo.bullet.bql.tree;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class CountDistinctNodeTest extends ExpressionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new CountDistinctNode(Collections.singletonList(new LiteralNode(5))),
                              new CountDistinctNode(Collections.emptyList()));
    }
}
