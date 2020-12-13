package ods;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
import java.util.Properties;

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: ods
 * ClassName: BaseDBMaxwell01
 *
 * @author 18729 created on date: 2020/12/9 15:14
 * @version 1.0
 * @since JDK 1.8
 */
public class BaseDBMaxwell01 {
    public static void main(String[] args) throws Exception {

        String kafkaBrokers = "hadoop01:9092";
        String zkBrokers = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
        String topic = "ODS_DB_GMALL2020_M";
        String groupId = "gmall_base_db_maxwell_group";

        System.out.println("===============》 flink任务开始  ==============》");

        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        //自定义端口
        conf.setInteger(RestOptions.PORT, 8050);
        //本地env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);
        properties.setProperty("zookeeper.connect",zkBrokers);
        properties.setProperty("group.id",groupId);

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//
//        env.enableCheckpointing(5000);
//        env.setStateBackend( new MemoryStateBackend());

        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer010 = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        DataStreamSource<String> stringDataStreamSource = env.addSource(stringFlinkKafkaConsumer010);
        String mess=null;
        SingleOutputStreamOperator<String> map = stringDataStreamSource.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                System.out.println(">>>>>>接收topic报文:" + s + "<<<<<");
                return s;
            }
        });

        map.addSink(new util.MyKafkaSink01());

        env.execute();
    }
}
