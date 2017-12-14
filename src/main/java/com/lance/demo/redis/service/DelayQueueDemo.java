package com.lance.demo.redis.service;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Set;

public class DelayQueueDemo {
    private static final String DELAY_QUEUE = "delay_queue";
    final Jedis jedis;

    public DelayQueueDemo(Jedis jedis) {
        this.jedis = jedis;
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
                Set<Tuple> item = jedis.zrangeWithScores(DELAY_QUEUE, 0, 0);
                if(item != null && !item.isEmpty()){
                    Tuple tuple = item.iterator().next();
                    if(System.currentTimeMillis() >= tuple.getScore()){
                        // TODO 获取锁
                        jedis.zrem(DELAY_QUEUE, tuple.getElement()); // 从延时队列中移除
                        process(tuple.getElement()); //任务推入延时队列，因为这里只是延时
                        // TODO 释放锁
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            Thread.sleep(500);

        }
    }

    private void process(String element) {
        System.out.println("process" + element);
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
