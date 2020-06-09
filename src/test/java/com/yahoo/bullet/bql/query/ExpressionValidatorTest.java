/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.parser.ParsingException;
import org.testng.annotations.Test;

public class ExpressionValidatorTest {
    @Test(expectedExceptions = ParsingException.class, expectedExceptionsMessageRegExp = ".*Literal missing its type\\.")
    public void testLiteral() {
        new ExpressionValidator(null, null).visitLiteral(null, null);
    }
}
