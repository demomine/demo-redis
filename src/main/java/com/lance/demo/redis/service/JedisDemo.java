package com.lance.demo.redis.service;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisDemo {
    private static final String host = "192.168.1.72";
    private static final int port = 6379;
    private static final int db = 1;
    private final Jedis jedis;

    public JedisDemo() {
        this(false);
        // jedis.auth(""); 设置密码
    }

    public JedisDemo(boolean pooled) {
        if (pooled) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(20);
            jedisPoolConfig.setMaxIdle(5);
            JedisPool jedisPool = new JedisPool(jedisPoolConfig, host, port);
            jedis = jedisPool.getResource();
        } else {
            jedis = new Jedis(host, port);
        }
        jedis.select(db);
    }

    public void set(String key, String value) {
        jedis.set(key, value);
        jedis.close();
    }

    public void set(String key, String value, long expire) {
        jedis.set(key, value);
        jedis.close();
    }

    public String get(String key) {
        return null;
    }

    public String hset(String key, String value) {
        return null;
    }

    public String hset(String key, String value, long expire) {
        return null;
    }

    public void pub() {
        jedis.publish("demo-channel1", "demo-channel1 hello redis pub sub");
        jedis.publish("demo-channel2", "demo-channel2 hello redis pub sub");
    }

    public void sub() {
        // jedis.subscribe(new JedisPubSubDemo(),"demo-channel1");
        jedis.subscribe(new JedisPubSubDemo(),"demo-channel1","demo-channel2");
    }
}
