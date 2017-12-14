package com.lance.demo.redis.service;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DelayQueueDemo {
    private static final String DELAY_QUEUE = "delay_queue";
    final JedisLock lock;
    final Jedis jedis;

    public DelayQueueDemo(Jedis jedis,JedisLock jedisLock) {
        this.jedis = jedis;
        this.lock = jedisLock;
    }

    public void delay(List<Task> tasks) {
        tasks.forEach(this::addDelay);
    }

    private void addDelay(Task task) {
        jedis.zadd(DELAY_QUEUE,task.getTimestamp(),task.getDesc());
        System.out.println("put"+task);
    }

    public void transferFromDelayQueue() throws InterruptedException{
        while(true){
            try {
                lock.acquire();
                Set<Tuple> item = jedis.zrangeWithScores(DELAY_QUEUE, 0, 3);
                List<String> tasks = new ArrayList<>();
                if(item != null && !item.isEmpty()){
                    item.forEach(tuple -> {
                        if(System.currentTimeMillis() >= tuple.getScore()){
                            jedis.zrem(DELAY_QUEUE, tuple.getElement()); // 从延时队列中移除
                            tasks.add(tuple.getElement());
                        }
                    });
                }
                lock.release();
                process(tasks);
            } catch (Exception e) {
                e.printStackTrace();
            }

            Thread.sleep(500);

        }
    }

    private void process(List<String> element) {
        element.forEach(task-> System.out.println("process" + task));
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static class Task{
        private String name;
        private double timestamp;
        private String desc;

        public Task(String name, double timestamp, String desc) {
            this.name = name;
            this.timestamp = timestamp;
            this.desc = desc;
        }

        public String getName() {
            return name;
        }

        public double getTimestamp() {
            return timestamp;
        }

        public String getDesc() {
            return desc;
        }

        @Override
        public String toString() {
            return "Task{" +
                    "name='" + name + '\'' +
                    ", timestamp=" + timestamp +
                    ", desc='" + desc + '\'' +
                    '}';
        }
    }
}
