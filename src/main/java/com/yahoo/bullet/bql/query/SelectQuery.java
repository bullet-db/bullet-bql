package com.yahoo.bullet.bql.query;

import com.yahoo.bullet.bql.BQLConfig;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Query;

import java.util.ArrayList;
import java.util.List;

import static com.yahoo.bullet.bql.extractor.QueryExtractor.extractDuration;
import static com.yahoo.bullet.bql.extractor.QueryExtractor.extractFilter;
import static com.yahoo.bullet.bql.extractor.QueryExtractor.extractWindow;

public class SelectQuery extends ClassifiedQuery {
    private List<BulletError> errors = new ArrayList<>();

    public SelectQuery(ProcessedQuery processedQuery) {
        super(processedQuery);
        process();
    }

    private void process() {

    }

    @Override
    public List<BulletError> validate() {
        if (!errors.isEmpty()) {
            return errors;
        }




        return errors;
    }

    @Override
    public Query extractQuery(BQLConfig config) {


        Query query = new Query();




        query.setAggregation(new Aggregation(processedQuery.getLimit(), Aggregation.Type.RAW));






        // duration
        // filter
        // projection
        // postaggs
        // window





        extractDuration(processedQuery, query, config.getAs(BulletConfig.QUERY_MAX_DURATION, Long.class));
        extractFilter(processedQuery, query);



        extractProjection(processedQuery, query);
        extractPostAggregations(processedQuery, query);


        extractWindow(processedQuery, query);

        return query;
    }
}
