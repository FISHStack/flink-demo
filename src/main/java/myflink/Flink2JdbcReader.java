package myflink;

import java.sql.DriverManager;
import java.sql.ResultSet;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Flink2JdbcReader {

    public static void main(String[] args) throws Exception {
        //sql查询结果列类型
        TypeInformation[] fieldTypes = new TypeInformation[]{
                BasicTypeInfo.STRING_TYPE_INFO,        //第一列数据类型
                BasicTypeInfo.STRING_TYPE_INFO     //第二类数据类型
//                BasicTypeInfo.INT_TYPE_INFO,
//                BasicTypeInfo.STRING_TYPE_INFO,
//                BasicTypeInfo.BIG_DEC_TYPE_INFO
        };

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                //数据库连接信息
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://192.168.3.202:3306/dev_smartoilets?useSSL=false&useUnicode=true&characterEncoding=UTF-8")
                .setUsername("root")
                .setPassword("password")
                .setQuery("SELECT device_id,device_name FROM t_device")//查询sql
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        //        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        //搭建flink
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataSet s1 = env.createInput(jdbcInputFormat); // datasource
//    System.out.println("hello");
        System.out.println("数据行：" + s1.count());
        s1.print();//此处打印
    }

}
