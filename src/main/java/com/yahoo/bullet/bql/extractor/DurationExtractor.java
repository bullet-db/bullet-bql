package com.yahoo.bullet.bql.extractor;

import com.yahoo.bullet.bql.tree.StreamNode;

public class DurationExtractor {
    private static final String MAX = "MAX";

    public static Long extractDuration(StreamNode streamNode) {
        String timeDuration = streamNode.getTimeDuration();
        if (timeDuration != null) {
            return timeDuration.equalsIgnoreCase(MAX) ? Long.MAX_VALUE : Long.parseLong(timeDuration);
        }
        return null;
    }
}
