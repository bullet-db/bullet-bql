/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.bql;

import com.yahoo.bullet.common.BulletConfig;
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
     * @throws Exception when bql is not valid.
     */
    public static void main(String[] args) throws Exception {
        //String bql = args[0];
        while (true) {
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                String bql = reader.readLine();
                log.debug("BQL passed in: " + bql);
                System.out.println();
                System.out.println("############################## Bullet Query ##############################");
                System.out.println();
                System.out.println(BULLET_QUERY_BUILDER.buildJson(bql));
                System.out.println();
                System.out.println("##########################################################################");
                System.out.println();
            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }
}
