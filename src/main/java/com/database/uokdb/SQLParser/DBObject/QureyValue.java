package com.database.uokdb.SQLParser.DBObject;

/**
 * Created by Zloj on 22.11.2015.
 */
public class QureyValue {
    String Value;
    QueryColumn Column;

    public QureyValue(String Value){
        this.Value = Value;
    }

    public QureyValue(QueryColumn Column, String Value){
        this.Column = Column;
        this.Value = Value;
    }

    public QueryColumn getColumn(){
        return this.Column;
    }
    public String getValue(){
        return this.Value;
    }
}
