package com.setapi.JavaUtils;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by ShellMount on 2019/8/1
 **/

public class JedisPoolUtil {

    public static volatile JedisPool jedisPool = null;
    private JedisPoolUtil(){}

    static {
        if (jedisPool == null) {
            InputStream insStream = JedisPoolUtil.class.getClassLoader()
                    .getResourceAsStream("redis.properties");
            Properties props = new Properties();

            try {
                props.load(insStream);
            } catch (IOException e) {
                e.printStackTrace();
            }

            GenericObjectPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(Integer.valueOf(props.getProperty("redis.maxTotal")));
            poolConfig.setMaxIdle(Integer.valueOf(props.getProperty("redis.maxIdle")));
            poolConfig.setMinIdle(Integer.valueOf(props.getProperty("redis.minIdle")));

            jedisPool = new JedisPool(
                    poolConfig,
                    props.getProperty("redis.url"),
                    Integer.valueOf(props.getProperty("redis.port"))
            );
        }
    }

    public static Jedis getJedisPoolInstance() {return jedisPool.getResource();}

    public static void release(Jedis jedis) {
        if (null != jedis) {
            jedis.close();
        }
    }

}
