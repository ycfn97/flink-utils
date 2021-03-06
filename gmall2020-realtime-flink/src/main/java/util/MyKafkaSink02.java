package util;

import bean.OrderInfo01;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: util
 * ClassName: MyKafkaSink02
 *
 * @author 18729 created on date: 2020/12/13 22:10
 * @version 1.0
 * @since JDK 1.8
 */
public class MyKafkaSink02 extends RichSinkFunction<OrderInfo01> {
    KafkaProducer<String, String> producer = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        Properties props = new Properties();
        //kafka集群，broker-list
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.put("acks", "all");
        //重试次数
        props.put("retries",10);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 1);
        //RecordAccumulator缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer=new KafkaProducer<String,String>(props);
    }
    @Override
    public void invoke(OrderInfo01 value,Context context) throws Exception {
        producer.send(new ProducerRecord("DWD_ORDER_INFO",JSONObject.toJSON(value).toString()));
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }
}
