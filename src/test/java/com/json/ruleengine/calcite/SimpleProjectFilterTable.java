package com.json.ruleengine.calcite;


import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.List;

/**
 * @Classname SimpleTable2Test
 * @Date 2018/11/15 下午11:48
 * @Create by yaolihua
 * @Description
 */
public class SimpleProjectFilterTable extends AbstractTable implements ProjectableFilterableTable {
    protected final Object[][] rows = {
            {1, "ao", 10},
            {2, "aka", 5},
            {3, "aka", 7}
    };

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelProtoDataType protoRowType = new RelProtoDataType() {
            public RelDataType apply(final RelDataTypeFactory a0) {
                return a0.builder()
                        .add("id", SqlTypeName.INTEGER)
                        .add("color", SqlTypeName.VARCHAR, 5)
                        .add("units", SqlTypeName.INTEGER)
                        .build();
            }
        };
        return protoRowType.apply(typeFactory);
    }

    public String toString() {
        return "PartsTable";
    }

    public Enumerable<Object[]> scan(final DataContext root) {
        return Linq4j.asEnumerable(rows);
    }

    public Enumerable<Object[]> scan(DataContext dataContext, List<RexNode> filters) {

        final String[] filterValues = new String[rows.length];
        filters.removeIf(filter -> addFilter(filter, filterValues));

        return Linq4j.asEnumerable(rows);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext dataContext, List<RexNode> filter, int[] projects) {
        //filter 是 where 条件的集合
        //在这里可以根据 filters 的条件过滤rows，得到过滤后的结果集，减少上层进行其它运算的数据集

        // 为谓词，即是选择的字段信息
        // 过滤需要的字段，减少运算数据集


        return Linq4j.asEnumerable(rows);
    }

    private boolean addFilter(RexNode filter, Object[] filterValues) {
        if (filter.isA(SqlKind.EQUALS)) {
            final RexCall call = (RexCall) filter;
            RexNode left = call.getOperands().get(0);
            if (left.isA(SqlKind.CAST)) {
                left = ((RexCall) left).operands.get(0);
            }
            final RexNode right = call.getOperands().get(1);
            if (left instanceof RexInputRef
                    && right instanceof RexLiteral) {
                final int index = ((RexInputRef) left).getIndex();
                if (filterValues[index] == null) {
                    filterValues[index] = ((RexLiteral) right).getValue2().toString();
                    return true;
                }
            }
        }
        return false;
    }

    public static void main(String[] args) throws Exception
    {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        SimpleProjectFilterTable ft = new SimpleProjectFilterTable();
        rootSchema.add("ft", ft);
        ResultSet result = connection.createStatement().executeQuery("SELECT \"id\",\"units\" FROM \"ft\"" +
                " where \"id\">2");
        System.out.println("-------"+result.getFetchSize());
        while(result.next()) {
            System.out.println(result.getObject(0));
        }
    }


}