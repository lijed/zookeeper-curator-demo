/*
 * Copyright 2020 tu.cn All right reserved. This software is the
 * confidential and proprietary information of tu.cn ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Tu.cn
 */
package com.me.learn.zk.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.imps.CreateBuilderImpl;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.concurrent.CountDownLatch;

import static com.me.learn.zk.curator.ZkConfig.SERVER_STR;

/**
 * @author Administrator
 * @date 2020/9/6 21:22
 * Project Name: zookeeper-demo
 */
public class CuratorDemo {


    public static void main(String[] args) throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(SERVER_STR)
                .sessionTimeoutMs(5000)  //会话超时时间
                .retryPolicy(new ExponentialBackoffRetry(1000, 3)) //1000 initial amount of time to wait between retries
                .connectionTimeoutMs(4000) //连接超时是时间
                .build();
        curatorFramework.start();

//        createZkNode(curatorFramework, "/first_node/first_node_sub_1", "test");

//        getZkNode(curatorFramework, "/first_node");

        createZkNodeWithAync(curatorFramework, "/test1", "test");
    }

    private static void createZkNode(CuratorFramework curatorFramework, String path, String data) throws Exception {
        String createBuilder = curatorFramework.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
        System.out.println("createBuilder = " + createBuilder);
    }

    private static void getZkNode(CuratorFramework curatorFramework, String path) throws Exception {
        byte[] result = curatorFramework.getData().forPath(path);
        System.out.println("path: " + path + " = " + new String(result));
    }

    private static String createZkNodeWithAync(CuratorFramework curatorFramework, String path, String data) throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        curatorFramework.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .inBackground(new BackgroundCallback() {
                    @Override
                    public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                        System.out.println(Thread.currentThread().getName() + "- state: " + event.getStat());
                        countDownLatch.countDown();
                    }
                  }
                )
                .forPath(path, data.getBytes());
        // add some logics
        System.out.println("before create node request is submitted");
        countDownLatch.await();
        System.out.println("after: create node request is processed");
        return null;
    }
}
