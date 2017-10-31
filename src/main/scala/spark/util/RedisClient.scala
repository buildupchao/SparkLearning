package spark.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by yachao on 17/10/28.
  */
object RedisClient extends Serializable {
  private var MAX_IDLE: Int = 200
  private var TIME_OUT: Int = 10000
  private var TEST_ON_BORROW: Boolean = true

  lazy val config: JedisPoolConfig = {
    val config = new JedisPoolConfig
    config.setMaxIdle(MAX_IDLE)
    config.setTestOnBorrow(TEST_ON_BORROW)
    config
  }

  var pool = new JedisPool(config, KafkaRedisProperties.REDIS_SERVER,
    KafkaRedisProperties.REDIS_PORT, TIME_OUT)

  var hook = new Thread {
    override def run(): Unit = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }

  sys.addShutdownHook(hook.run)
}
