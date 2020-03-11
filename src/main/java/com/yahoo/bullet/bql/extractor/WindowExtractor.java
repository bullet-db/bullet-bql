/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.tree.WindowIncludeNode;
import com.yahoo.bullet.parsing.Window;

import java.util.HashMap;
import java.util.Map;

import static com.yahoo.bullet.parsing.Window.EMIT_EVERY_FIELD;
import static com.yahoo.bullet.parsing.Window.INCLUDE_FIRST_FIELD;
import static com.yahoo.bullet.parsing.Window.TYPE_FIELD;

public class WindowExtractor {
    static Window extractWindow(ProcessedQuery processedQuery) {
        Map<String, Object> emit = new HashMap<>();
        emit.put(EMIT_EVERY_FIELD, processedQuery.getWindow().getEmitEvery());
        emit.put(TYPE_FIELD, processedQuery.getWindow().getEmitType().toString());

        WindowIncludeNode windowInclude = processedQuery.getWindow().getWindowInclude();

        if (windowInclude == null) {
            return new Window(emit, null);
        }

        Map<String, Object> include = new HashMap<>();
        include.put(TYPE_FIELD, windowInclude.getIncludeUnit().toString());
        include.put(INCLUDE_FIRST_FIELD, windowInclude.getFirst());

        return new Window(emit, include);
    }
}
