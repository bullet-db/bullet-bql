package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class NullPredicateNodeTest extends ExpressionNodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new NullPredicateNode(identifier("abc"), true),
                              new NullPredicateNode(identifier("def"), true),
                              new NullPredicateNode(identifier("abc"), false));
    }
}
