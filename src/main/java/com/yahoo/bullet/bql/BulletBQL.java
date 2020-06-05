/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.Query;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@Slf4j
public class BulletBQL {
    /**
     * Print out a Bullet JSON by parsing BQL using default BulletConfig.
     *
     * @param args A BQL string.
     */
    public static void main(String[] args) {
        BQLConfig config = new BQLConfig();
        config.set(BulletConfig.RECORD_SCHEMA_FILE_NAME, "src/test/resources/test_schema.json");
        config.validate();

        BulletQueryBuilder builder = new BulletQueryBuilder(config);

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            try {
                String line = reader.readLine();
                if (line.isEmpty()) {
                    return;
                }
                BQLResult result = builder.buildQuery(line);
                if (result.hasErrors()) {
                    System.out.println(result.getErrors());
                    continue;
                }
                Query query = result.getQuery();
                System.out.println(query);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
