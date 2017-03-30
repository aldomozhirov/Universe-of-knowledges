package com.database.uokdb.SQLParser.DBObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Zloj on 20.11.2015.
 */
public class QueryTable {
    String Name;
    String Alias;
    List Columns;

    public QueryTable(String Name){
        this.Name = Name;
        this.Alias = Name;
        Columns = new ArrayList<QueryColumn>();
    }
    public QueryTable(String Name, String Alias){
        this.Name = Name;
        this.Alias = Alias;
        Columns = new ArrayList<QueryColumn>();
    }

    public String getName(){
        return Name;
    }
    public String getAlias(){
        return Alias;
    }
    public void setAlias(String Alias){
        this.Alias = Alias;
    }

    public void addColumn(String Column){
        QueryColumn column = new QueryColumn(Column);
        Columns.add(column);
    }
    public void addColumn(String Column, String Alias){
        QueryColumn column = new QueryColumn(Column, Alias);
        Columns.add(column);
    }
    public void addColumn(QueryColumn column){
        Columns.add(column);
    }
    public void addAllColumns(List columns){
        for(Object col:columns){
            Columns.add((QueryColumn)col);
        }
    }
}
