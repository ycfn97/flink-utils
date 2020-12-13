package util;

import com.alibaba.fastjson.JSON;
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
 * ClassName: MyKafkaSink01
 *
 * @author 18729 created on date: 2020/12/9 20:42
 * @version 1.0
 * @since JDK 1.8
 */
public class MyKafkaSink01 extends RichSinkFunction<String> {
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
    public void invoke(String value, Context context) throws Exception {
        JSONObject jsonObject = JSON.parseObject(value);
        String table = jsonObject.getString("table");
        String data = jsonObject.getString("data");
//        System.out.println(new Tuple2(table,data));
        producer.send(new ProducerRecord("ODS_"+table.toUpperCase(),data));
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }

}
