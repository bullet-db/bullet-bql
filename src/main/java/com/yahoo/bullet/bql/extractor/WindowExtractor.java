/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.query.ProcessedQuery;
import com.yahoo.bullet.bql.tree.WindowIncludeNode;
import com.yahoo.bullet.bql.tree.WindowNode;
import com.yahoo.bullet.query.Window;

public class WindowExtractor {
    static Window extractWindow(ProcessedQuery processedQuery) {
        WindowNode window = processedQuery.getWindow();
        if (window == null) {
            return new Window();
        }
        WindowIncludeNode windowInclude = window.getWindowInclude();
        if (windowInclude == null) {
            return new Window(window.getEmitEvery(), window.getEmitType());
        }
        return new Window(window.getEmitEvery(), window.getEmitType(), windowInclude.getIncludeUnit(), windowInclude.getFirst());
    }
}
