package com.lance.demo.redis.service;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class DelayQueueDemoTest {

    private Jedis jedis;
    private JedisPool pool;
    private JedisLock jedisLock;
    private static final String QUEUE_NAME = "deplay_queue";

    @Before
    public void setUp() {
        pool = new JedisPool(new JedisPoolConfig(), "120.26.164.41",6379,60000,"Xinb@918testRedis");
        jedis = pool.getResource();
        jedisLock = new JedisLock(jedis, "delay_key", 1000, 30000);
    }

    @After
    public void after() {
        jedis.close();
        pool.destroy();
    }

    @Test
    public void delay() throws Exception{
        DelayQueueDemo delayQueueDemo = new DelayQueueDemo(jedis,jedisLock);
        List<DelayQueueDemo.Task> tasks = getTask();
        delayQueueDemo.delay(tasks);
        delayQueueDemo.transferFromDelayQueue();

    }

    private List<DelayQueueDemo.Task> getTask() {
        double time = System.currentTimeMillis();
        List<DelayQueueDemo.Task> tasks = new ArrayList<>();

        DelayQueueDemo.Task task1 = new DelayQueueDemo.Task("task1", time, "task 1 process");
        DelayQueueDemo.Task task2 = new DelayQueueDemo.Task("task2", time=time+10000D, "task 2 process");
        DelayQueueDemo.Task task3 = new DelayQueueDemo.Task("task3", time=time+1000D, "task 3 process");
        DelayQueueDemo.Task task4 = new DelayQueueDemo.Task("task4", time=time+1000D, "task 4 process");
        DelayQueueDemo.Task task5 = new DelayQueueDemo.Task("task5", time=time+1000D, "task 5 process");
        DelayQueueDemo.Task task6 = new DelayQueueDemo.Task("task6", time=time+1000D, "task 6 process");
        DelayQueueDemo.Task task7 = new DelayQueueDemo.Task("task7", time=time+10000D, "task 7 process");
        DelayQueueDemo.Task task8 = new DelayQueueDemo.Task("task8", time=time+1000D, "task 8 process");
        DelayQueueDemo.Task task9 = new DelayQueueDemo.Task("task9", time=time+1000D, "task 9 process");

        tasks.add(task1);
        tasks.add(task2);
        tasks.add(task3);
        tasks.add(task4);
        tasks.add(task5);
        tasks.add(task6);
        tasks.add(task7);
        tasks.add(task8);
        tasks.add(task9);

        return tasks;
    }

}