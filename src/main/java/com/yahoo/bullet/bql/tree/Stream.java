/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Stream extends QueryBody {
    private final Optional<String> timeDuration;
    private final Optional<String> recordDuration;

    /**
     * Constructor that requires an Optional of String timeDuration and an Optional of String recordDuration.
     *
     * @param timeDuration   An Optional of String.
     * @param recordDuration An Optional of String.
     */
    public Stream(Optional<String> timeDuration, Optional<String> recordDuration) {
        this(Optional.empty(), timeDuration, recordDuration);
    }

    /**
     * Constructor that requires a {@link NodeLocation}, an Optional of String timeDuration and an Optional of String recordDuration.
     *
     * @param location       A {@link NodeLocation}.
     * @param timeDuration   An Optional of String.
     * @param recordDuration An Optional of String.
     */
    public Stream(NodeLocation location, Optional<String> timeDuration, Optional<String> recordDuration) {
        this(Optional.of(location), timeDuration, recordDuration);
    }

    private Stream(Optional<NodeLocation> location, Optional<String> timeDuration, Optional<String> recordDuration) {
        super(location);
        this.timeDuration = requireNonNull(timeDuration, "timeDuration is null");
        this.recordDuration = requireNonNull(recordDuration, "recordDuration is null");
    }

    /**
     * Get the {@link #timeDuration} of this Stream.
     *
     * @return An Optional of String.
     */
    public Optional<String> getTimeDuration() {
        return timeDuration;
    }

    /**
     * Get the {@link #recordDuration} of this Stream.
     *
     * @return An Optional of String.
     */
    public Optional<String> getRecordDuration() {
        return recordDuration;
    }

    @Override
    public <R, C> R accept(ASTVisitor<R, C> visitor, C context) {
        return visitor.visitStream(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("timeDuration", timeDuration)
                .add("recordDuration", recordDuration)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Stream obj = (Stream) o;
        return Objects.equals(timeDuration, obj.timeDuration) &&
                Objects.equals(recordDuration, obj.recordDuration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeDuration, recordDuration);
    }
}
