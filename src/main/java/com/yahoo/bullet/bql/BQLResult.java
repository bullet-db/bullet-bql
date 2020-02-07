package com.yahoo.bullet.bql;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Query;
import lombok.Getter;

import java.util.List;

@Getter
public class BQLResult {
    private Query query;
    private List<BulletError> errors;

    BQLResult(Query query) {
        this.query = query;
    }

    BQLResult(List<BulletError> errors) {
        this.errors = errors;
    }

    /**
     * Returns whether or not there are errors.
     *
     * @return True if there are errors and false otherwise.
     */
    public boolean hasErrors() {
        return errors != null;
    }
}
