package com.zhengxw.gmall.realtime.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.zhengxw.gmall.common.constants.GmallConstant
import com.zhengxw.gmall.realtime.bean.StartUpLog
import com.zhengxw.gmall.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis


object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_STARTUP, ssc)

//    inputDstream.foreachRDD{rdd =>
//      println(rdd.map(record => record.value()).collect().mkString("\n"))
//
//    }
    // 將inputDstream 处理成startuplog对象
    val startUpLogDStream: DStream[StartUpLog] = inputDstream.map { record =>
      val jsonString: String = record.value()
      val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
      val formattor: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateHour: String = formattor.format(new Date(startUpLog.ts))
      val dateHourArr: Array[String] = dateHour.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      startUpLog

    }

    //3 根据清单进行过滤
    //driver
    val filteredDStream: DStream[StartUpLog] = startUpLogDStream.transform { rdd =>
      //driver   每批次执行一次
      println("过滤前：" + rdd.count())

      val jedis = RedisUtil.getJedisClient
      val dateString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

      val dauKey = "dau:" + dateString
      val dauMidSet: util.Set[String] = jedis.smembers(dauKey)
      val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)

      val filteredRdd: RDD[StartUpLog] = rdd.filter { startuplog =>
        val dauMidSet: util.Set[String] = dauMidBC.value
        val flag: Boolean = dauMidSet.contains(startuplog.mid)
        !flag
      }

      println("过滤后：" + filteredRdd.count())

      filteredRdd

    }






    //用户访问清单保存到redis
    /**
     * 代码优化    优化redis链接
     */
    filteredDStream.foreachRDD{rdd=>

      rdd.foreachPartition{startupItr=>
        //executor 执行一次
        val jedis = RedisUtil.getJedisClient
//        val jedis = new Jedis("hadoop102", 6379)
        for(startup <- startupItr){
          //executor 反复执行
          val dauKey = "dau"+startup.logDate

          jedis.sadd(dauKey,startup.mid)
        }

        jedis.close()


      }
    }

    //用户访问清单保存到redis
//    startUpLogDStream.foreachRDD{rdd=>
//
//      rdd.foreach(startuplog => {
//        //保存redis的操作   所有今天访问过的mid
//        // redis 1 type;set   2 key:[dau:date]  3 value:mid
//        val jedis = new Jedis("hadoop102", 6379)
//
//        val dauKey = "dau"+startuplog.logDate
//
//        jedis.sadd(dauKey,startuplog.mid)
//
//        jedis.close()
//
//      })
//    }


    ssc.start()

    ssc.awaitTermination()

  }
}
