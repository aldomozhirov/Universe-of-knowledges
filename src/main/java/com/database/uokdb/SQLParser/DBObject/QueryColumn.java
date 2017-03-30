package com.database.uokdb.SQLParser.DBObject;

/**
 * Created by Zloj on 20.11.2015.
 */
public class QueryColumn {
    String Name;
    String Alias;
    QueryTable ParentTable;

    public QueryColumn(String Name){
        this.Name = Name;
    }
    public QueryColumn(String Name, String Alias){
        this.Name = Name;
        this.Alias = Alias;
    }

    public String getName(){
        return Name;
    }
    public String getAlias(){
        return Alias;
    }
    public QueryTable getParentTable(){
        return ParentTable;
    }
    public void setAlias(String Alias){
        this.Alias = Alias;
    }
    public void setParentTable(QueryTable ParentTable){
        this.ParentTable = ParentTable;
    }


}
