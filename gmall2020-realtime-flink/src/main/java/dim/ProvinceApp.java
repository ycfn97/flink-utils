package dim;

import bean.dim.ProvinceInfo;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import util.PhoenixUtil;

import java.util.Properties;

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: dim
 * ClassName: ProvinceApp
 *
 * @author 18729 created on date: 2020/12/13 12:46
 * @version 1.0
 * @since JDK 1.8
 */
public class ProvinceApp {
    public static void main(String[] args) throws Exception {
        String kafkaBrokers = "hadoop01:9092";
        String zkBrokers = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
        String topic = "ODS_BASE_PROVINCE";
        String groupId = "province_group";

        System.out.println("===============》 flink任务开始  ==============》");


        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        //自定义端口
        conf.setInteger(RestOptions.PORT, 8070);
        //本地env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);
        //生产env
        //val env = StreamExecutionEnvironment.getExecutionEnvironment

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置kafka连接参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);
        properties.setProperty("zookeeper.connect", zkBrokers);
        properties.setProperty("group.id", groupId);
        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置检查点时间间隔
        env.enableCheckpointing(5000);
        env.setStateBackend( new MemoryStateBackend());
        //创建kafak消费者，获取kafak中的数据
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        DataStreamSource<String> kafkaData = env.addSource(kafkaConsumer);
        DataStream<String> userData = kafkaData.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) {
//                System.out.println(">>>>>>接收topic报文:"+s+"<<<<<");
                return s;
            }
        });

        userData.map(new RichMapFunction<String, ProvinceInfo>() {
            @Override
            public ProvinceInfo map(String value) throws Exception {
                ProvinceInfo o = (ProvinceInfo) JSON.parseObject(value, Class.forName("bean.dim.ProvinceInfo"));
                new PhoenixUtil().update("upsert into GMALL2020_PROVINCE_INFO values('" +
                        o.getId()+"','"+o.getName()+"','"+o.getRegion_id()+"','"+o.getArea_code()+"','"+o.getIso_code()+"','"+o.getIso_3166_2()+"')");
                return o;
            }
        }).print();

        env.execute();
    }
}
