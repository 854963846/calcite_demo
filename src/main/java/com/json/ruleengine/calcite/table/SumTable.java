package com.json.ruleengine.calcite.table;

import com.alibaba.fastjson.JSONObject;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.ArrayList;
import java.util.List;

/**
 * @Classname CalculateTable
 * @Date 2018/11/9 下午4:36
 * @Create by yaolihua
 * @Description
 */
public class SumTable extends AbstractTable implements ScannableTable {


    private JSONObject table;

    public SumTable(JSONObject sumTable) {
        this.table = sumTable;
    }

    public SumTable() {
    }

    public String getTableName() {
        return this.table.getString("table");
    }

    private static List<Object[]> dataList;
    static {
        //data
        dataList = new ArrayList<Object[]>();
        Object[] rows = new Object[3];
        rows[0] = "ylh"; //name
        rows[1] = "19890920";   //age
        rows[2] = "engine"; //sex
        dataList.add(rows);
    }

    public Enumerable<Object[]> scan(DataContext dataContext) {
        return Linq4j.asEnumerable(dataList);
    }

    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        List<RelDataType> typeList = new ArrayList<RelDataType>();
        typeList.add(relDataTypeFactory.createJavaType(String.class));
        typeList.add(relDataTypeFactory.createJavaType(String.class));
        typeList.add(relDataTypeFactory.createJavaType(Integer.class));

        List<String> fieldNameList = new ArrayList<String>();
        fieldNameList.add("name");
        fieldNameList.add("birthday");
        fieldNameList.add("job");

        RelDataType structType = relDataTypeFactory.createStructType(typeList, fieldNameList);
        return structType;
    }

}
