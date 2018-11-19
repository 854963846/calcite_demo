package com.json.ruleengine.calcite;

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
public class SimpleTableTest {
    @org.junit.Test
    public void testMain() throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        Hr hr = new Hr();
        Address[] addresses = new Address[1];
        Address address = new Address(1,1);
        addresses[0] = address;

        hr.emps[0] = new Employee(100, "Bill", addresses);

        address = new Address(2,2);
        addresses[0] = address;
        hr.emps[1] = new Employee(200, "Eric", addresses);

        address = new Address(3,3);
        addresses[0] = address;
        hr.emps[2] = new Employee(150, "Sebastian", addresses);

        ReflectiveSchema hrSchema = new ReflectiveSchema(hr);
        rootSchema.add("hr", hrSchema);
        Statement statement = connection.createStatement();
        ResultSet resultSet =
                statement.executeQuery("select *\n"
                        + "from \"hr\".\"emps\".\"address\"");
        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            int n = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= n; i++) {
                buf.append(i > 1 ? "; " : "")
                        .append(resultSet.getMetaData().getColumnLabel(i))
                        .append("=")
                        .append(resultSet.getObject(i));
            }
            System.out.println(buf.toString());
            buf.setLength(0);
        }
        resultSet.close();
        statement.close();
        connection.close();
    }

    /** Object that will be used via reflection to create the "hr" schema. */
    public static class Hr {
        public final Employee[] emps = new Employee[3];
    }

    /** Object that will be used via reflection to create the "emps" table. */
    public static class Employee {
        public final int empid;
        public final String name;
        public Address[] address = new Address[1];

        public Employee(int empid, String name, Address[] address) {
            this.empid = empid;
            this.name = name;
            this.address = address;
        }
    }

    public static class Address {
        public final int street;
        public final int road;

        public Address(int street, int road)
        {
            this.street = street;
            this.road = road;
        }
    }
}
