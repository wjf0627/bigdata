package com.jinfeng.util;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.oss.driver.api.core.AsyncPagingIterable;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @package: com.jinfeng.util
 * @author: wangjf
 * @date: 2020/9/25
 * @time: 11:08 上午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class CassandraUtil {


    public static Demo test(CqlSession cqlSession, String devid) {
        //  AtomicReference<String> ver = new AtomicReference<>("");
        //  CopyOnWriteArrayList<Set<String>> res = new CopyOnWriteArrayList<>();
        AtomicReference<Demo> demo = new AtomicReference<>();
        //  CompletionStage<CqlSession> sessionStage = CqlSession.builder().buildAsync();

        // Chain one async operation after another:
        CompletionStage<AsyncResultSet> responseStage = cqlSession.executeAsync("select devid,region from rtdmp.device_region where devid = '" + devid + "'");

        CompletionStage<Row> resultStage =
                responseStage.thenApply(AsyncPagingIterable::one
                );

        resultStage.whenComplete(
                (row, error) -> {
                    if (error != null) {
                        System.out.printf("Failed to retrieve the version: %s%n", error.getMessage());
                    } else {
                        String dev_id = row.getString("devid");
                        Set<String> region = (Set<String>) row.getObject("region");
                        //  vera.set(version);
                        System.out.println("devid -->> " + dev_id + "," + region);
                        //  res.add(region);
                        demo.set(new Demo(dev_id, region));
                    }
                    cqlSession.closeAsync();
                });
        return demo.get();
    }

    public static CopyOnWriteArrayList<Demo> res = new CopyOnWriteArrayList<>();

    public static CompletionStage<Row> testV2(String devid) {
        //  AtomicReference<String> ver = new AtomicReference<>("");
        //  CopyOnWriteArrayList<Set<String>> res = new CopyOnWriteArrayList<>();
        //  AtomicReference<Demo> demo = new AtomicReference<>();
        CompletionStage<CqlSession> sessionStage2 = CqlSession.builder().buildAsync();

        // Chain one async operation after another:
        CompletionStage<AsyncResultSet> responseStage =
                sessionStage2.thenCompose(
                        session -> session.executeAsync("select devid,region from rtdmp.device_region where devid = '" + devid + "'"));

        CompletionStage<Row> resultStage =
                responseStage.thenApply(AsyncPagingIterable::one
                );

        CompletionStage<Row> rowCompletionStage = resultStage.whenComplete(
                (row, error) -> {
                    if (error != null) {
                        System.out.printf("Failed to retrieve the version: %s%n", error.getMessage());
                    } else {
                        String dev_id = row.getString("devid");
                        Set<String> region = (Set<String>) row.getObject("region");
                        System.out.println(dev_id + "," + region);
                        new Demo(dev_id, region);
                    }
                    sessionStage2.thenAccept(CqlSession::closeAsync);
                });
        return rowCompletionStage;
    }

    public static List<Demo> rows = new ArrayList<>();

    public static void testV3(String devid) {
        CompletionStage<CqlSession> sessionStage = CqlSession.builder().buildAsync();

        CompletionStage<AsyncResultSet> responseStage =
                sessionStage.thenComposeAsync(
                        session -> session.executeAsync("select devid,region from rtdmp.device_region where devid = '" + devid + "'"));
        CompletableFuture<Demo> resultStage =
                responseStage.thenApplyAsync(resultSet -> {
                    if (resultSet != null) {
                        Row row = resultSet.one();
                        String dev_id = row.getString("devid");
                        Set<String> region = (Set<String>) row.getObject("region");
                        System.out.println(dev_id + "," + region);
                        return new Demo(dev_id, region);
                    } else {
                        return null;
                    }
                }).toCompletableFuture();


        resultStage.whenComplete(
                (row, error) -> {
                    if (error != null && row == null) {
                        System.out.printf("Failed to retrieve the version: %s%n", error.getMessage());
                    } else {
                        System.out.println(row.devid + "," + row.region);
                        //  res.add(region);
                    }
                    sessionStage.thenAcceptAsync(CqlSession::closeAsync);
                });
    }

    static void processRows(AsyncResultSet rs, Throwable error) {
        if (error != null) {
            System.out.printf("Failed to retrieve the version: %s%n", error.getMessage());
        } else {
            Row row = rs.one();
            String dev_id = rs.one().getString("devid");
            Set<String> region = (Set<String>) row.getObject("region");
            System.out.println(dev_id + "," + region);
            rows.add(new Demo(dev_id, region));
        }
    }

    public static CompletableFuture<Demo> testV4(CqlSession sessionStage, String devid) {
        //  CompletionStage<CqlSession> sessionStage = CqlSession.builder().buildAsync();

        //
        //  ResultSetFuture future = (ResultSetFuture) sessionStage.executeAsync("");
        CompletionStage<AsyncResultSet> responseStage = sessionStage.executeAsync("select devid,region from rtdmp.device_region where devid = '" + devid + "'");
        //  sessionStage.thenComposeAsync(
        //      session -> session.executeAsync("select devid,region from rtdmp.device_region where devid = '" + devid + "'"));
        CompletableFuture<Demo> resultStage =
                responseStage.thenApplyAsync(resultSet -> {
                    if (resultSet != null) {
                        Row row = resultSet.one();
                        String dev_id = row.getString("devid");
                        Set<String> region = (Set<String>) row.getObject("region");
                        //  System.out.println(dev_id + "," + region);
                        return new Demo(dev_id, region);
                    } else {
                        return null;
                    }
                }).toCompletableFuture();

        return resultStage.toCompletableFuture();
    }

    public static CompletableFuture<Demo> testV5(String devid) {
        CompletionStage<CqlSession> sessionStage = CqlSession.builder().buildAsync();

        CompletionStage<AsyncResultSet> responseStage =
                sessionStage.thenComposeAsync(
                        session -> session.executeAsync("select devid,region from rtdmp.device_region where devid = '" + devid + "'"));
        CompletionStage<Demo> resultStage =
                responseStage.thenApplyAsync(resultSet -> {
                    if (resultSet != null) {
                        Row row = resultSet.one();
                        String dev_id = row.getString("devid");
                        Set<String> region = (Set<String>) row.getObject("region");
                        //  System.out.println(dev_id + "," + region);
                        return new Demo(dev_id, region);
                    } else {
                        return null;
                    }
                });

        resultStage.whenComplete((row, error) -> sessionStage.thenAcceptAsync(CqlSession::closeAsync));
        return resultStage.toCompletableFuture();
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String[] strs = new String[]{"1f23d864-3fb7-470b-9912-ebd4e681ebaf", "fa5004c4-fed8-4368-9b6f-4d163c9d5d44", "9cda161f-da05-4fb0-9c19-90a122539032"};
        List<CompletionStage<Demo>> completionStages = Lists.newArrayList();
        for (String str : strs) {
            completionStages.add(testV5(str));
        }
        //  completableFutures
        CompletableFutures.allSuccessful(completionStages);
        for (CompletionStage<Demo> completionStage : completionStages) {

            System.out.println(completionStage.toCompletableFuture().get().devid);
        }
    }
}