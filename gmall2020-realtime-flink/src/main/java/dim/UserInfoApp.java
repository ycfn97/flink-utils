package dim;

import bean.dim.UserInfo;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import util.PhoenixUtil;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: dim
 * ClassName: UserInfoApp
 *
 * @author 18729 created on date: 2020/12/13 16:21
 * @version 1.0
 * @since JDK 1.8
 */
public class UserInfoApp {
    public static void main(String[] args) throws Exception {
        String kafkaBrokers = "hadoop01:9092";
        String zkBrokers = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
        String topic = "ODS_USER_INFO";
        String groupId = "gmall_user_info_group";

        System.out.println("===============》 flink任务开始  ==============》");


        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        //自定义端口
        conf.setInteger(RestOptions.PORT, 8090);
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
//        env.enableCheckpointing(5000);
//        env.setStateBackend( new MemoryStateBackend());
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

        userData.map(new RichMapFunction<String, UserInfo>() {
            @Override
            public UserInfo map(String value) throws Exception {
                UserInfo o = (UserInfo) JSON.parseObject(value, Class.forName("bean.dim.UserInfo"));
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date date = simpleDateFormat.parse(o.getBirthday());
                long l = System.currentTimeMillis();
                long l1 = l - date.getTime();
                long age = l1 / 1000L / 60L / 60L / 24L / 365L;
                if (age<20){
                    o.setAge_group("20岁及以下");
                }else if (age>30){
                    o.setAge_group("30岁以上");
                }else {
                    o.setAge_group("21岁到30岁");
                }
                if (o.getGender().equals("M")){
                    o.setGender_name("男");
                }else {
                    o.setGender_name("女");
                }

                new PhoenixUtil().update("upsert into GMALL_USER_INFO values('" +
                        o.getId()+"','"+o.getUser_level()+"','"+o.getBirthday()+"','"+o.getGender()+"','"+o.getAge_group()+"','"+o.getGender_name()+"')");
                return o;
            }
        }).print();

        env.execute();
    }
}
