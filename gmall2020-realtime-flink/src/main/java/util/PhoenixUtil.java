package util;

import com.alibaba.fastjson.JSONObject;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Copyright(c) 2020-2021 sparrow All Rights Reserved
 * Project: gmall2020-parent
 * Package: util
 * ClassName: PhoenixUtil
 *
 * @author 18729 created on date: 2020/12/10 14:16
 * @version 1.0
 * @since JDK 1.8
 */
public class PhoenixUtil {
    Connection connection=null;
    public static void main(String[] args) throws SQLException {
        PhoenixUtil phoenixUtil = new PhoenixUtil();
//        List<JSONObject> jsonObjects = phoenixUtil.queryList("select * from \"GMALL2020_DAU\"");
//        System.out.println(jsonObjects);
        phoenixUtil.update("upsert into STUDENT values('1001','zhangsan','beijing')");
    }

    public PhoenixUtil() throws SQLException {
        String url = "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181";
        Properties props = new Properties();
        props.put("phoenix.schema.isNamespaceMappingEnabled","true");
        if (connection==null){
            connection= DriverManager.getConnection(url,props);
        }
    }

    public void update(String sql) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.executeUpdate();
        connection.commit();
    }

    public List<JSONObject> queryList(String sql) throws SQLException {
        List<JSONObject> jsonObjects = new LinkedList<>();
//        "select * from \"GMALL2020_DAU\""
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while(rs.next()){
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <=metaData.getColumnCount(); i++) {
                jsonObject.put(metaData.getColumnName(i),rs.getObject(i));
            }
            jsonObjects.add(jsonObject);
        }
        ps.close();
        connection.close();
        return jsonObjects;
    }
}
