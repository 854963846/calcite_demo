package com.json.ruleengine.calcite;

import com.json.ruleengine.calcite.table.CalculateTable;
import com.json.ruleengine.calcite.table.SumTable;
import com.json.ruleengine.vo.People;
import com.json.ruleengine.vo.PeopleSchema;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

/**
 * @Classname SchemaManageTest
 * @Date 2018/11/9 下午5:25
 * @Create by yaolihua
 * @Description
 */
public class SchemaManageTest {

    @org.junit.Test
    public void testMain() throws Exception {
//        CalculateTable table = new CalculateTable();
//        SumTable sumTable = new SumTable();
//        SchemaManage manage = new SchemaManage();
//        SchemaPlus rootSchema = manage.getRootSchema();
//        rootSchema.add("CT", table);
//        rootSchema.add("ST", sumTable);
//        ResultSet result = manage.executeSQL("SELECT CT.\"name\", \"age\", \"sex\", \"birthday\", \"job\" FROM CT JOIN ST ON CT.\"name\"=ST.\"name\"");
//        while(result.next()) {
//            System.out.println(result.getString("name") + "\t" +
//                    result.getString("age") + "\t" +
//                    result.getString("sex") + "\t" +
//                    result.getString("birthday") + "\t" +
//                    result.getString("job")
//            );
//        }

    }

    @org.junit.Test
    public void testT() throws Exception {
//        Connection connection =
//                DriverManager.getConnection("jdbc:calcite:");
//        CalciteConnection calciteConnection =
//                connection.unwrap(CalciteConnection.class);
//        SchemaPlus rootSchema = calciteConnection.getRootSchema();
//        PeopleSchema peopleSchema = new PeopleSchema();
//        People p = new People();
//        p.getInfo().put("name", "ylh");
//        p.getInfo().put("age", 29);
//        p.getInfo().put("sex", "male");
//        peopleSchema.peoples[0] = p;
//        rootSchema.add("ps", new ReflectiveSchema(peopleSchema));
//        ResultSet result = connection.createStatement().executeQuery("select \"name\" from \"ps\".\"peoples\"");
//        System.out.println("-------"+result.getFetchSize());
//        while(result.next()) {
//            System.out.println(result.getObject(0)
//            );
//        }
    }




}