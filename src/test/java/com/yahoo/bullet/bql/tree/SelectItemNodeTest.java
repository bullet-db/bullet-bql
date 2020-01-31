package com.yahoo.bullet.bql.tree;

import org.testng.annotations.Test;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;

public class SelectItemNodeTest extends NodeTest {
    @Test
    public void testEqualsAndHashCode() {
        testEqualsAndHashCode(() -> new SelectItemNode(false, new LiteralNode(5), identifier("abc")),
                              new SelectItemNode(true, new LiteralNode(5), identifier("abc")),
                              new SelectItemNode(false, new LiteralNode(true), identifier("abc")),
                              new SelectItemNode(false, new LiteralNode(5), identifier("def")));
    }
}
