package com.json.ruleengine.calcite;

import com.json.ruleengine.vo.People;
import com.json.ruleengine.vo.PeopleSchema;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @Classname SimpleTableTest
 * @Date 2018/11/15 下午11:40
 * @Create by yaolihua
 * @Description
 */
public class SimpleTable3Test {
    @org.junit.Test
    public void testMain() throws Exception {
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        PeopleSchema peopleSchema = new PeopleSchema();
        People p = new People();
        p.setName("ylh");
        p.setAge(29);
        p.setSex("male");
        peopleSchema.peoples[0] = p;
        rootSchema.add("ps", new ReflectiveSchema(peopleSchema));
        ResultSet result = connection.createStatement().executeQuery("SELECT \"name\" FROM \"ps\".\"peoples\"");
        System.out.println("-------"+result.getFetchSize());
        while(result.next()) {
            System.out.println(result.getObject(0));
        }
    }
}
