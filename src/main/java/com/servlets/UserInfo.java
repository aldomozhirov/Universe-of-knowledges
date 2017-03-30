package com.servlets;

import com.database.DBinteraction;
import com.database.PostgreSQL;

import java.util.HashMap;
import java.util.List;

/**
 * Created by Alexey on 17.11.2015.
 */
public class UserInfo {

    public static List<HashMap<String, Object>> result;
    public static String query;
    public static int page, pages;
    public static DBinteraction database = new PostgreSQL();
    public static String database_name = "PostgreSQL";

}
