/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/test/java/com/facebook/presto/sql/tree/TestLikePredicate.java
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.yahoo.bullet.bql.util.QueryUtil.identifier;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class LikePredicateTest {
    private ExpressionNode value;
    private ExpressionNode patterns;
    private LikePredicate likePredicate;

    @BeforeClass
    public void setUp() {
        value = identifier("aaa");
        patterns = new ListExpressionNode(singletonList(identifier("bbb")));
        likePredicate = new LikePredicate(value, patterns);
    }

    @Test
    public void testEquals() {
        LikePredicate copy = likePredicate;
        assertTrue(likePredicate.equals(copy));
        assertFalse(likePredicate.equals(null));
        assertFalse(likePredicate.equals(value));

        ExpressionNode diffValue = identifier("ccc");
        LikePredicate likePredicateDiffValue = new LikePredicate(diffValue, patterns);
        assertFalse(likePredicate.equals(likePredicateDiffValue));

        ExpressionNode diffPatterns = new ListExpressionNode(singletonList(identifier("ddd")));
        LikePredicate likePredicateDiffPatterns = new LikePredicate(value, diffPatterns);
        assertFalse(likePredicate.equals(likePredicateDiffPatterns));

        LikePredicate likePredicateDiffEscape = new LikePredicate(value, patterns, Optional.of(identifier("fff")));
        assertFalse(likePredicate.equals(likePredicateDiffEscape));
    }

    @Test
    public void testHashCode() {
        LikePredicate sameLikePredicate = new LikePredicate(identifier("aaa"), patterns);
        assertEquals(likePredicate.hashCode(), sameLikePredicate.hashCode());
    }

    @Test
    public void testGetChildren() {
        StringLiteralNode value = new StringLiteralNode("a");
        StringLiteralNode pattern = new StringLiteralNode("b");
        Optional<ExpressionNode> escape = Optional.of(new StringLiteralNode("c"));

        assertEquals(new LikePredicate(value, pattern, escape).getChildren(), ImmutableList.of(value, pattern, escape.get()));
        assertEquals(new LikePredicate(value, pattern, Optional.empty()).getChildren(), ImmutableList.of(value, pattern));
    }
}
