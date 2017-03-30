package com.servlets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Alexey on 21.10.2015.
 */
public class QueryConstructor {

    protected List<String> tables, conditions;
    protected List<HashMap<String, Object>> result;
    protected String order;
    protected String projection;

    QueryConstructor(){
        tables = new ArrayList<String>();
        conditions = new ArrayList<String>();
        result = new ArrayList<HashMap<String, Object>>();
        projection = "*";
        order = null;
    }

    public void add_table(String name, String litera) {
        tables.add(name + " " + litera);
    }

    public void add_conditions(String... condition) {
        for(String c : condition)
            conditions.add(c);
    }

    public void set_order(String order){
        this.order = order;
    }

    public void set_projection(String projection){
        this.projection = projection;
    }

    public void execute() {
        if(tables.isEmpty())
            return;
        Iterator<String> iter;
        String query_string = "select "+ projection + " from ";
        iter = tables.iterator();
        while(iter.hasNext()) {
            query_string +=  iter.next();
            if(iter.hasNext())
                query_string += ", ";
        }
        if(!conditions.isEmpty()) {
            query_string += " where ";
            iter = conditions.iterator();
            while (iter.hasNext()) {
                query_string += iter.next();
                if (iter.hasNext())
                    query_string += " and ";
            }
        }
        if(order != null)
            query_string += " order by " + order;
        query_string += ";";
        System.out.println(query_string);
        UserInfo.database.open();
        result = UserInfo.database.query(query_string);
        UserInfo.database.close();
    }

    public void clear(){
        tables.clear();
        conditions.clear();
        result.clear();
        order = null;
        projection = null;
    }

    public List<HashMap<String, Object>> getResult() {
        return result;
    }
}
