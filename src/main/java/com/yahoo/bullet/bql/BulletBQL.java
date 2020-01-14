/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Query;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@Slf4j
public class BulletBQL {
    private static BulletQueryBuilder BULLET_QUERY_BUILDER = new BulletQueryBuilder(new BulletConfig());

    /**
     * Print out a Bullet JSON by parsing BQL using default BulletConfig.
     *
     * @param args A BQL string.
     */
    public static void main(String[] args) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        BulletConfig config = new BulletConfig();
        while (true) {
            try {
                String line = reader.readLine();
                if (line.isEmpty()) {
                    return;
                }
                Query query = BULLET_QUERY_BUILDER.buildQuery(line);
                query.configure(config);
                System.out.println(BULLET_QUERY_BUILDER.toJson(query));
                System.out.println(query.initialize());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
