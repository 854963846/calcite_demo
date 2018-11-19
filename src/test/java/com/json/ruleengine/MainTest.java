package com.json.ruleengine;

import com.json.ruleengine.calcite.SimpleFilterTable;
import com.json.ruleengine.vo.People;
import com.json.ruleengine.vo.PeopleSchema;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

/**
 * @Classname MainTest
 * @Date 2018/11/9 下午2:31
 * @Create by yaolihua
 * @Description
 */
public class MainTest {


    @org.junit.Test
    public void testMain() throws Exception {
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        SimpleFilterTable ft = new SimpleFilterTable();

        ResultSet result = connection.createStatement().executeQuery("SELECT \"name\" FROM \"ps\".\"peoples\"");
        System.out.println("-------"+result.getFetchSize());
        while(result.next()) {
            System.out.println(result.getObject(0));
        }
    }
}
