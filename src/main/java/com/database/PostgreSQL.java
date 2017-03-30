/**
 * Created by Alexey on 17.11.2015.
 */

package com.database;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PostgreSQL implements DBinteraction {

    private static Connection dbConnection = null;

    public void open()
    {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        try {
            dbConnection =
                    DriverManager.getConnection("jdbc:postgresql://localhost:5432/UOK", "postgres", "km1861ol");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void close()
    {
        if (dbConnection != null)
            try {
                dbConnection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }

    public void inputData(String SQL_query)
    {
        Statement statement = null;
        try {
            statement = dbConnection.createStatement();
            statement.executeUpdate(SQL_query);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public List<HashMap<String, Object>> query(String SQL_query)
    {
        Statement statement;
        ResultSet resultset;
        List<HashMap<String, Object>> resultlist = new ArrayList<HashMap<String, Object>>();
        try {
            statement = dbConnection.createStatement();
            resultset = statement.executeQuery(SQL_query);
            if(resultset != null)
                while(resultset.next()) {
                    HashMap<String, Object> tuple = new HashMap<String, Object>();
                    for(int i = 0; i < resultset.getMetaData().getColumnCount(); i++)
                        tuple.put(resultset.getMetaData().getColumnName(i + 1), resultset.getObject(i + 1));
                    resultlist.add(tuple);
                }

        } catch (SQLException e) {
            System.out.println("Error!");
        }
        return resultlist;
    }
}
