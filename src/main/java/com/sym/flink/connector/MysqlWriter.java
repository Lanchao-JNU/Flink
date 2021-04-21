package com.sym.flink.connector;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhanglanchao
 * \* Date: 2021/4/15
 * \* Time: 12:33 下午
 * \* Description:
 * \
 */
public class MysqlWriter extends RichSinkFunction<Tuple3<String, String,Long>> {
    private Connection connection = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (connection == null) {
            Class.forName("com.mysql.cj.jdbc.Driver");//加载数据库驱动
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "Asd806866316");//获取连接
        }

        ps = connection.prepareStatement("insert into steam.dm_steam_game_info_year values (?,?,?)");
        System.out.println("123123");
    }

    @Override
    public void invoke(Tuple3<String, String,Long> value, Context context) throws Exception {
        //获取JdbcReader发送过来的结果
        try {
            ps.setString(1, value.f0);
            ps.setString(2, value.f1);
            ps.setLong(3, value.f2);
            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (ps != null) {
            ps.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}