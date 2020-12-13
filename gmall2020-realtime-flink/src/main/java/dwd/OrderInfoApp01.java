package dwd;

import bean.OrderInfo01;
import bean.dim.ProvinceInfo;
import bean.dim.UserInfo;
import bean.dim.UserState;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.http.HttpHost;
import org.apache.jute.Index;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import util.MyKafkaSink02;
import util.PhoenixUtil;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: dwd
 * ClassName: OrderInfoApp01
 *
 * @author 18729 created on date: 2020/12/10 12:22
 * @version 1.0
 * @since JDK 1.8
 */
public class OrderInfoApp01 {
    public static void main(String[] args) throws Exception {
        String kafkaBrokers = "hadoop01:9092";
        String zkBrokers = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
        String topic = "ODS_ORDER_INFO";
        String groupId = "order_info_group";

        System.out.println("===============》 flink任务开始  ==============》");


        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        //自定义端口
        conf.setInteger(RestOptions.PORT, 8060);
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

        SingleOutputStreamOperator<OrderInfo01> map = userData.map(new RichMapFunction<String, OrderInfo01>() {
            @Override
            public OrderInfo01 map(String value) throws Exception {
                OrderInfo01 o = (OrderInfo01) JSON.parseObject(value, Class.forName("bean.OrderInfo01"));
                String[] s = o.getCreate_time().split(" ");
                o.setCreate_date(s[0]);
                String[] split = s[1].split(":");
                o.setCreate_hour(split[0]);
                return o;
            }
        });

        SingleOutputStreamOperator<OrderInfo01> map1 = map.map(new RichMapFunction<OrderInfo01, OrderInfo01>() {
            @Override
            public OrderInfo01 map(OrderInfo01 value) throws Exception {
                value.setIf_first_order("1");
                Long user_id = value.getUser_id();
                String s = "select user_id,if_consumed from USER_STATE2020 where user_id = '"+user_id+"'";
                List<JSONObject> jsonObjects = new PhoenixUtil().queryList(s);
//                System.out.println(jsonObjects);
                for (JSONObject jsonObject:jsonObjects) {
//                    System.out.println(jsonObject.getString("USER_ID"));
//                    System.out.println(jsonObject.getString("IF_CONSUMED"));
                    if (jsonObject.getString("USER_ID").equals(value.getUser_id().toString())&&"1".equals(jsonObject.getString("IF_CONSUMED"))){
//                        System.out.println(111111111);
                        value.setIf_first_order("0");
                        break;
                    }
                }
//                System.out.println(value);
                return value;
            }
        });

        SingleOutputStreamOperator<OrderInfo01> map2 = map1.map(new RichMapFunction<OrderInfo01, OrderInfo01>() {
            @Override
            public OrderInfo01 map(OrderInfo01 value) throws Exception {
                String sql = "select  id,name,region_id,area_code,iso_code,iso_3166_2 from GMALL2020_PROVINCE_INFO ";
                List<JSONObject> jsonObjects = new PhoenixUtil().queryList(sql);
                HashMap<String, ProvinceInfo> provinceInfoHashMap = new HashMap<>();
                for (JSONObject object : jsonObjects) {
                    String id = object.getString("ID");
                    ProvinceInfo provinceInfo = new ProvinceInfo(object.getString("ID"), object.getString("NAME"), object.getString("REGION_ID"), object.getString("AREA_CODE"), object.getString("ISO_CODE"), object.getString("ISO_3166_2"));
                    provinceInfoHashMap.put(id, provinceInfo);
                }
//                System.out.println(provinceInfoHashMap.get(value.getProvince_id().toString()));
                value.setProvince_name(provinceInfoHashMap.get(value.getProvince_id().toString()).getName());
                value.setProvince_area_code(provinceInfoHashMap.get(value.getProvince_id().toString()).getArea_code());
                value.setProvince_iso_code(provinceInfoHashMap.get(value.getProvince_id().toString()).getIso_code());
                value.setProvince_iso_3166_2(provinceInfoHashMap.get(value.getProvince_id().toString()).getIso_3166_2());
                return value;
            }
        });

        map2.filter(new FilterFunction<OrderInfo01>() {
            @Override
            public boolean filter(OrderInfo01 value) throws Exception {
                return value!=null&&value.getUser_id()!=null;
            }
        });

        SingleOutputStreamOperator<OrderInfo01> map3 = map2.map(new RichMapFunction<OrderInfo01, OrderInfo01>() {
            @Override
            public OrderInfo01 map(OrderInfo01 value) throws Exception {
                String sql = "select id,user_level,birthday,gender,age_group,gender_name from GMALL_USER_INFO ";
                List<JSONObject> jsonObjects = new PhoenixUtil().queryList(sql);
                HashMap<String, UserInfo> userInfoHashMap = new HashMap<>();
                for (JSONObject object : jsonObjects) {
                    String id = object.getString("ID");
                    UserInfo userInfo = new UserInfo(object.getString("ID"), object.getString("USER_LEVEL"), object.getString("BIRTHDAY"), object.getString("GENDER"), object.getString("AGE_GROUP"), object.getString("GENDER_NAME"));
                    userInfoHashMap.put(id, userInfo);
                }
//                System.out.println(value.getUser_id());
//                System.out.println(userInfoHashMap.get(value.getUser_id().toString()));
//                System.out.println(userInfoHashMap.get(value.getUser_id().toString()).getAge_group());
                value.setUser_age_group(userInfoHashMap.get(value.getUser_id().toString()).getAge_group());
                value.setUser_gender(userInfoHashMap.get(value.getUser_id().toString()).getGender_name());
                return value;
            }
        });

        map3.filter(new FilterFunction<OrderInfo01>() {
            @Override
            public boolean filter(OrderInfo01 value) throws Exception {
                return value.getIf_first_order()=="1";
            }
        }).map(new RichMapFunction<OrderInfo01, OrderInfo01>() {
            @Override
            public OrderInfo01 map(OrderInfo01 value) throws Exception {
                new PhoenixUtil().update("upsert into USER_STATE2020 values('" +
                        value.getUser_id()+"','"+value.getIf_first_order()+"')");
                return value;
            }
        });

        map3.addSink(EsUtils().build());
        map3.addSink(new MyKafkaSink02());

        env.execute();
    }

    private static ElasticsearchSink.Builder<OrderInfo01> EsUtils() {
        String format = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop01", 9200, "http"));

        ElasticsearchSink.Builder<OrderInfo01> esBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<OrderInfo01>() {
            @Override
            public void process(OrderInfo01 orderInfo01, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                requestIndexer.add(createIndexRequest(orderInfo01));
            }

            private IndexRequest createIndexRequest(OrderInfo01 orderInfo01) {
                HashMap<String, Object> map = new HashMap<>();
                map.put("id", orderInfo01.getId());
                map.put("province_id", orderInfo01.getProvince_id());
                map.put("order_status", orderInfo01.getOrder_status());
                map.put("user_id", orderInfo01.getUser_id());
                map.put("final_total_amount", orderInfo01.getFinal_total_amount());
                map.put("benefit_reduce_amount", orderInfo01.getBenefit_reduce_amount());
                map.put("original_total_amount", orderInfo01.getOriginal_total_amount());
                map.put("feight_fee", orderInfo01.getFeight_fee());
                map.put("expire_time", orderInfo01.getExpire_time());
                map.put("create_time", orderInfo01.getCreate_time());
                map.put("create_hour", orderInfo01.getCreate_hour());
                map.put("if_first_order", orderInfo01.getIf_first_order());
                map.put("province_name", orderInfo01.getProvince_name());
                map.put("province_area_code", orderInfo01.getProvince_iso_3166_2());
                map.put("user_age_group", orderInfo01.getUser_age_group());
                map.put("user_gender", orderInfo01.getUser_gender());
                System.out.println("data:" + orderInfo01);
                return Requests.indexRequest()
                        .index("gmall2020_order_info_" + format)
                        .type("_doc")
                        .id(orderInfo01.getUser_id().toString())
                        .source(map);
            }
        });

        esBuilder.setBulkFlushMaxActions(1);
        esBuilder.setRestClientFactory(new util.RestClientFactoryImpl());
        esBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        return esBuilder;
    }
}
