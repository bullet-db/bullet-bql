/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.parser;

import org.testng.annotations.Test;

import java.util.HashSet;

public class DelimiterLexerTest {
    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "\\QnextToken() requires a non-null input stream\\E.*")
    public void testNextTokenNullInput() {
        DelimiterLexer lexer = new DelimiterLexer(null, new HashSet<>());
        lexer.nextToken();
    }
}
