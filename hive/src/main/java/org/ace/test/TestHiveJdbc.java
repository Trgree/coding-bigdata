package org.ace.test;

import java.sql.*;

/**
 * Created by Liangsj on 2018/2/27.
 */
public class TestHiveJdbc {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive2://192.168.5.151:10000/default","hive","");
        Statement stmt = conn.createStatement();
        String sql = "show databases";
        System.out.println("Running: " + sql);
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}
