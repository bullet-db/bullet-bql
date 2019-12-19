/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */

/*
 * Adapted and modified from the Presto project:
 * https://github.com/prestodb/presto/blob/1898faf2ec4881709c9b8197e8332f302d618875/presto-parser/src/main/java/com/facebook/presto/sql/testing/TreeAssertions.java
 */
package com.yahoo.bullet.bql.util;  

public final class TreeAssertions {
    /*private TreeAssertions() {
    }

    public static void assertFormattedBQL(BQLParser bqlParser, Node expected) {
        ParsingOptions parsingOptions = new ParsingOptions(AS_DOUBLE);
        assertFormattedBQL(bqlParser, parsingOptions, expected);
    }

    public static void assertFormattedBQLDecimal(BQLParser bqlParser, Node expected) {
        ParsingOptions parsingOptions = new ParsingOptions(AS_DECIMAL);
        assertFormattedBQL(bqlParser, parsingOptions, expected);
    }

    public static void assertFormattedBQL(BQLParser bqlParser, ParsingOptions parsingOptions, Node expected) {
        String formatted = formatBQL(expected);

        // Verify round-trip of formatting already-formatted BQL
        Statement actual = parseFormatted(bqlParser, parsingOptions, formatted, expected);
        assertEquals(formatBQL(actual), formatted);

        // Compare parsed tree with parsed tree of formatted BQL
        if (!actual.equals(expected)) {
            // Simplify finding the non-equal part of the tree
            assertListEquals(linearizeTree(actual), linearizeTree(expected));
        }
        assertEquals(actual, expected);
    }

    private static Statement parseFormatted(BQLParser bqlParser, ParsingOptions parsingOptions, String bql, Node tree) {
        try {
            return bqlParser.createStatement(bql, parsingOptions);
        } catch (ParsingException e) {
            throw new AssertionError(format(
                    "failed to parse formatted BQL: %s\nerror: %s\ntree: %s",
                    bql, e.getMessage(), tree));
        }
    }

    private static List<Node> linearizeTree(Node tree) {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        new DefaultTraversalVisitor<Node, Void>() {
            @Override
            public Node process(Node node, @Nullable Void context) {
                Node result = super.process(node, context);
                nodes.add(node);
                return result;
            }
        }.process(tree, null);
        return nodes.build();
    }

    private static <T> void assertListEquals(List<T> actual, List<T> expected) {
        if (actual.size() != expected.size()) {
            throw new AssertionError(format("Lists not equal in size%n%s", formatLists(actual, expected)));
        }
        if (!actual.equals(expected)) {
            throw new AssertionError(format("Lists not equal at index %s%n%s",
                    differingIndex(actual, expected), formatLists(actual, expected)));
        }
    }

    private static <T> String formatLists(List<T> actual, List<T> expected) {
        Joiner joiner = Joiner.on("\n    ");
        return format("Actual [%s]:%n    %s%nExpected [%s]:%n    %s%n",
                actual.size(), joiner.join(actual),
                expected.size(), joiner.join(expected));
    }

    private static <T> int differingIndex(List<T> actual, List<T> expected) {
        for (int i = 0; i < actual.size(); i++) {
            if (!actual.get(i).equals(expected.get(i))) {
                return i;
            }
        }
        return actual.size();
    }

    private static <T> void assertEquals(T actual, T expected) {
        if (!actual.equals(expected)) {
            throw new AssertionError(format("expected [%s] but found [%s]", expected, actual));
        }
    }*/
}
