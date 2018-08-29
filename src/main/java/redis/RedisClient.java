package redis;

import redis.clients.jedis.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * create by 1311230692@qq.com on 2018/8/29 13:52
 * redis 客户端类
 **/
public class RedisClient {
    /**
     * 非切片客户端连接
     * */
    private Jedis jedis;
    /**
     * 非切片连接池
     * */
    private JedisPool jedisPool;
    /**
     * 切片客户端连接池
     * */
    private ShardedJedis shardedJedis;
    /**
     * 切片连接池
     * */
    private ShardedJedisPool shardedJedisPool;

    public RedisClient() {
        initialPool();
        initialShardedPool();
        jedis = jedisPool.getResource();
        shardedJedis = shardedJedisPool.getResource();
        jedis.auth("liuke666");
    }

    /**
     * 初始化非切片池
     * */
    private void initialPool() {
        // 池基本配置
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(10001);
        config.setTestOnBorrow(false);

        jedisPool = new JedisPool(config,"127.0.0.1",6379);
    }

    /**
     * 初始化切片池
     * */
    private void initialShardedPool() {
        // 池基本配置
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(10001);
        config.setTestOnBorrow(false);

        // slave 链接
        List<JedisShardInfo> list = new ArrayList<JedisShardInfo>();
        JedisShardInfo jedisShardInfo = new JedisShardInfo("127.0.0.1",6379,"master");
        jedisShardInfo.setPassword("liuke666"); // shardedJedis 使用 JedisShardInfo 设置密码
        list.add(jedisShardInfo);
        // 构造池
        shardedJedisPool = new ShardedJedisPool(config,list);

    }

    public void show() {
        keyOperate();
        stringOperate();
        listOperate();
        setOperate();
        zsetOperate();
        hashOperate();
        jedisPool.returnResource(jedis);
        shardedJedisPool.returnResource(shardedJedis);
    }

    /**
     * key 操作
     * */
    private void keyOperate() {
        System.out.println("---------- key ------------");
        // 清空数据
        System.out.println("清空库中所有的数据：" + jedis.flushDB());
        // 判断 key 是否存在
        System.out.println("判断 key001 是否存在：" + shardedJedis.exists("key001"));
        System.out.println("新增 key002,value002 键值对：" + shardedJedis.set("key002","value002"));
        System.out.println("判断 key002 是否存在：" + shardedJedis.exists("key002"));
        // 输出系统中所有的key
        System.out.println("新增 key003,value003 键值对：" + shardedJedis.set("key003","value003"));
        System.out.println("系统中所有的键值对如下：");
        Set<String> keys = jedis.keys("*");
        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            System.out.println(key);
        }
        // 删除某个 key,若 key不存在，则忽略该命令
        System.out.println("系统中删除 key002" + jedis.del("key002"));
        System.out.println("判断 key002 是否存在" + shardedJedis.exists("key002"));
        // 设置 key003 的过期时间
        System.out.println("设置 key003 过期时间为 5 秒：" + jedis.expire("key003",5));
        try {
            Thread.sleep(3000);
        } catch (InterruptedException i) {
            i.printStackTrace();
        }
        // 查看某个 key 的剩余生存时间，单位【秒】，永久生存或不存在的都返回 -1 -2 表示过期且已删除，>0 表示举例过期还有多少秒或毫秒
        System.out.println("查看 key003 的剩余生存时间：" + jedis.ttl("key003"));
        // 移除某个 key 的生存时间
        System.out.println("移除 key003 的生存时间：" + jedis.persist("key003"));
        System.out.println("查看 key003 的剩余生存时间：" + jedis.ttl("key003"));
        // 查看 key 所存储的值得类型
        System.out.println("查看 key 所存储的类型：" + jedis.type("key003"));
    }

    /**
     * String 功能
     * */
    private void stringOperate() {
        System.out.println("----------- String_1 --------------");
        // 清空数据
        System.out.println("清空库中所有的数据：" + jedis.flushDB());

        System.out.println("------------- 新增 --------------");
        jedis.set("key001","value001");
        jedis.set("key002","value002");
        jedis.set("key003","value003");
        System.out.println("已新增的 3 个键值对如下：");
        System.out.println(jedis.get("key001"));
        System.out.println(jedis.get("key002"));
        System.out.println(jedis.get("key003"));
        System.out.println();

        System.out.println("-------------- 删除 -------------");
        System.out.println("删除 key003 键值对：" + jedis.del("key003"));
        System.out.println("查看 key003 键对应的值：" + jedis.get("key003"));
        System.out.println();

        System.out.println("------------- 修改 ------------");
        // 直接覆盖原来的数据
        System.out.println("直接覆盖 key001 原来的数据：" + jedis.set("key001","value001-update"));
        System.out.println("查看 key001 键对应的值：" + jedis.get("key001"));
        // 直接覆盖原来的数据
        System.out.println("在 key002 原来的值后面追加：" + jedis.append("key002","+appendStr"));
        System.out.println("查看 key002 键对应的值：" + jedis.get("key002"));
        System.out.println();

        System.out.println("-------------- 新增、删除、查找多个 ------------");
        System.out.println("一次性新增 key201,key202,key203,key204 及其对应值：" + jedis.mset("key201","value201",
                "key202","value202","key203","value203","key204","value204"));
        System.out.println("一次性获取 key201,key202,key203,key204 各自对应的值："+
                jedis.mget("key201","key202","key203","key204"));
        System.out.println("一次性删除 key201,key202：" + jedis.del(new String[]{"key201", "key202"}));
        System.out.println("一次性获取 key201,key202,key203,key204 各自对应的值：" +
                jedis.mget("key201","key202","key203","key204"));
        System.out.println();

        System.out.println("-------------- String_2 ---------------");
        // 清空数据
        System.out.println("清空库中所有数据：" + jedis.flushDB());
        System.out.println("------------新增键值对时防止覆盖原先值-------------");
        System.out.println("原先 key301 不存在时，新增 key301：" + shardedJedis.setnx("key301", "value301"));
        System.out.println("原先 key302 不存在时，新增 key302：" + shardedJedis.setnx("key302", "value302"));
        System.out.println("当 key302 存在时，尝试新增 key302：" + shardedJedis.setnx("key302", "value302_new"));
        System.out.println("获取 key301 对应的值：" + shardedJedis.get("key301"));
        System.out.println("获取 key302 对应的值：" + shardedJedis.get("key302"));

        System.out.println("---------- 超出有效期键值对被删除 -----------");
        // 设置 key 的有效期，并存储数据
        System.out.println("新增 key303，并指定过期时间为 2 秒" + shardedJedis.setex("key303", 2, "key303-2second"));
        System.out.println("获取 key303 对应的值：" + shardedJedis.get("key303"));
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("3 秒之后，获取 key303 对应的值：" + shardedJedis.get("key303"));

        System.out.println("-------- 获取原值，更新新值一步完成 ----------");
        System.out.println("key302 原值：" + shardedJedis.getSet("key302", "value302-after-getset"));
        System.out.println("key302 新值：" + shardedJedis.get("key302"));

        System.out.println("--------- 获取子串 ---------");
        System.out.println("获取 key302 对应值中的子串：" + shardedJedis.getrange("key302", 5, 7));
    }

    /**
     * List 功能
     * */
    private void listOperate() {
        System.out.println("----------- list -----------");
        // 清空数据
        System.out.println("-------- 新增 ----------");
        shardedJedis.lpush("stringlists", "vector");
        shardedJedis.lpush("stringlists", "ArrayList");
        shardedJedis.lpush("stringlists", "vector");
        shardedJedis.lpush("stringlists", "vector");
        shardedJedis.lpush("stringlists", "LinkedList");
        shardedJedis.lpush("stringlists", "MapList");
        shardedJedis.lpush("stringlists", "SerialList");
        shardedJedis.lpush("stringlists", "HashList");
        shardedJedis.lpush("numberlists", "3");
        shardedJedis.lpush("numberlists", "1");
        shardedJedis.lpush("numberlists", "5");
        shardedJedis.lpush("numberlists", "2");
        System.out.println("所有元素-stringlists：" + shardedJedis.lrange("stringlists", 0, -1));
        System.out.println("所有元素-numberlists：" + shardedJedis.lrange("numberlists", 0, -1));
        System.out.println("---------- 删除 ---------");
        System.out.println();

        // 删除列表指定的值 ，第二个参数为删除的个数（有重复时），后add进去的值先被删，类似于出栈
        System.out.println("成功删除指定元素个数-stringlists：" + shardedJedis.lrem("stringlists", 2, "vector"));
        System.out.println("删除指定元素之后-stringlists：" + shardedJedis.lrange("stringlists", 0, -1));
        // 删除区间以外的数据
        System.out.println("删除下标 0-3 区间之外的元素：" + shardedJedis.ltrim("stringlists", 0, 3));
        System.out.println("删除指定区间之外元素后-stringlists：" + shardedJedis.lrange("stringlists", 0, -1));
        // 列表元素出栈
        System.out.println("出栈元素：" + shardedJedis.lpop("stringlists"));
        System.out.println("元素出栈后-stringlists：" + shardedJedis.lrange("stringlists", 0, -1));
        System.out.println();

        System.out.println("---------- 修改 -----------");
        // 修改列表中指定下标的值
        shardedJedis.lset("stringlists", 0, "hello list!");
        System.out.println("下标为 0 的值修改后-stringlists：" + shardedJedis.lrange("stringlists", 0, -1));

        System.out.println("---------- 查找 -----------");
        // 数组长度
        System.out.println("长度-stringlists：" + shardedJedis.llen("stringlists"));
        System.out.println("长度-numberlists：" + shardedJedis.llen("numberlists"));

        // 排序
        /**
         * list中存字符串时必须指定参数为alpha，如果不使用SortingParams，而是直接使用sort("list")，
         * 会出现"ERR One or more scores can't be converted into double"
         * */
        SortingParams sortingParams = new SortingParams();
        sortingParams.alpha();
        sortingParams.limit(0, 3);
        System.out.println("返回排序后的结果-stringlists：" + shardedJedis.sort("stringlists", sortingParams));
        System.out.println("返回排序后的结果-numberlists：" + shardedJedis.sort("numberlists"));

        // 子串：  start为元素下标，end也为元素下标；-1代表倒数一个元素，-2代表倒数第二个元素
        System.out.println("子串-第二个开始到结束：" + shardedJedis.lrange("stringlists", 1, -1));
        // 获取指定下标的值
        System.out.println("获取下标为 2 的元素：" + shardedJedis.lindex("stringlists", 2)+"\n");
    }

    /**
     * set 操作
     * */
    private void setOperate() {
        System.out.println("---------- set -----------");
        System.out.println("清空库中所有数据：" + jedis.flushDB());

        System.out.println("---------- 新增 -----------");
        System.out.println("向sets集合中加入元素 element001：" + jedis.sadd("sets", "element001"));
        System.out.println("向sets集合中加入元素 element002：" + jedis.sadd("sets", "element002"));
        System.out.println("向sets集合中加入元素 element003：" + jedis.sadd("sets", "element003"));
        System.out.println("向sets集合中加入元素 element004：" + jedis.sadd("sets", "element004"));
        System.out.println("查看sets集合中的所有元素:" + jedis.smembers("sets"));
        System.out.println();

        System.out.println("--------- 删除 -----------");
        System.out.println("集合 sets 中删除元素 element003：" + jedis.srem("sets", "element003"));
        System.out.println("查看 sets 集合中的所有元素:" + jedis.smembers("sets"));
        System.out.println();

        System.out.println("--------- 修改 ----------");
        System.out.println();

        System.out.println("---------- 查找 ----------");
        System.out.println("判断 element001 是否在集合 sets 中："+jedis.sismember("sets", "element001"));
        System.out.println("循环查询获取 sets 中的每个元素：");
        Set<String> set = jedis.smembers("sets");
        Iterator<String> it = set.iterator();

        while (it.hasNext()) {
            Object obj = it.next();
            System.out.println(obj);
        }
        System.out.println();

        System.out.println("------- 集合运算 -------");
        System.out.println("sets1 中添加元素 element001：" + jedis.sadd("sets1", "element001"));
        System.out.println("sets1 中添加元素 element002：" + jedis.sadd("sets1", "element002"));
        System.out.println("sets1 中添加元素 element003：" + jedis.sadd("sets1", "element003"));
        System.out.println("sets2 中添加元素 element002：" + jedis.sadd("sets2", "element002"));
        System.out.println("sets2 中添加元素 element003：" + jedis.sadd("sets2", "element003"));
        System.out.println("sets2 中添加元素 element004：" + jedis.sadd("sets2", "element004"));
        System.out.println("查看 sets1 集合中的所有元素:" + jedis.smembers("sets1"));
        System.out.println("查看 sets2 集合中的所有元素:" + jedis.smembers("sets2"));
        System.out.println("sets1 和 sets2 交集：" + jedis.sinter("sets1", "sets2"));
        System.out.println("sets1 和 sets2 并集：" + jedis.sunion("sets1", "sets2"));
        System.out.println("sets1 和 sets2 差集：" + jedis.sdiff("sets1", "sets2"));//差集：set1中有，set2中没有的元素
    }

    /**
     * SortedSet 有序集合
     * */
    private void zsetOperate() {
        System.out.println("--------- zset --------");
        // 清空数据
        System.out.println(jedis.flushDB());

        System.out.println("-------- 新增 ----------");
        System.out.println("zset 中添加元素 element001：" + shardedJedis.zadd("zset", 7.0, "element001"));
        System.out.println("zset 中添加元素 element002：" + shardedJedis.zadd("zset", 8.0, "element002"));
        System.out.println("zset 中添加元素 element003：" + shardedJedis.zadd("zset", 2.0, "element003"));
        System.out.println("zset 中添加元素 element004：" + shardedJedis.zadd("zset", 3.0, "element004"));
        System.out.println("zset 集合中的所有元素：" + shardedJedis.zrange("zset", 0, -1));//按照权重值排序
        System.out.println();

        System.out.println("-------- 删除 ----------");
        System.out.println("zset 中删除元素 element002：" + shardedJedis.zrem("zset", "element002"));
        System.out.println("zset 集合中的所有元素：" + shardedJedis.zrange("zset", 0, -1));
        System.out.println();

        System.out.println("-------- 修改 ----------");
        System.out.println();

        System.out.println("-------- 查找 ----------");
        System.out.println("统计 zset 集合中的元素中个数：" + shardedJedis.zcard("zset"));
        System.out.println("统计 zset 集合中权重某个范围内（1.0——5.0），元素的个数：" + shardedJedis.zcount("zset", 1.0, 5.0));
        System.out.println("查看 zset 集合中element004的权重：" + shardedJedis.zscore("zset", "element004"));
        System.out.println("查看下标 1 到 2 范围内的元素值：" + shardedJedis.zrange("zset", 1, 2));
    }

    /**
     * Hash 功能
     * */
    private void hashOperate() {
        System.out.println("-------- hash --------");
        // 清空数据
        System.out.println("清空库中所有数据：" + jedis.flushDB());

        System.out.println("------- 新增 --------");
        System.out.println("hashs 中添加key001和value001键值对：" + shardedJedis.hset("hashs", "key001", "value001"));
        System.out.println("hashs 中添加key002和value002键值对：" + shardedJedis.hset("hashs", "key002", "value002"));
        System.out.println("hashs 中添加key003和value003键值对：" + shardedJedis.hset("hashs", "key003", "value003"));
        System.out.println("新增 key004 和 4 的整型键值对：" + shardedJedis.hincrBy("hashs", "key004", 4l));
        System.out.println("hashs 中的所有值：" + shardedJedis.hvals("hashs"));
        System.out.println();

        System.out.println("------- 删除 --------");
        System.out.println("hashs 中删除 key002 键值对：" + shardedJedis.hdel("hashs", "key002"));
        System.out.println("hashs 中的所有值：" + shardedJedis.hvals("hashs"));
        System.out.println();

        System.out.println("------- 修改 --------");
        System.out.println("key004 整型键值的值增加 100：" + shardedJedis.hincrBy("hashs", "key004", 100l));
        System.out.println("hashs 中的所有值：" + shardedJedis.hvals("hashs"));
        System.out.println();

        System.out.println("------- 查找 --------");
        System.out.println("判断 key003 是否存在：" + shardedJedis.hexists("hashs", "key003"));
        System.out.println("获取 key004 对应的值：" + shardedJedis.hget("hashs", "key004"));
        System.out.println("批量获取 key001 和 key003 对应的值：" + shardedJedis.hmget("hashs", "key001", "key003"));
        System.out.println("获取 hashs 中所有的 key：" + shardedJedis.hkeys("hashs"));
        System.out.println("获取 hashs 中所有的 value：" + shardedJedis.hvals("hashs"));
        System.out.println();
    }
}
