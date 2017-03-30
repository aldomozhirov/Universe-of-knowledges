package com.database.uokdb.SQLParser.SQLConditions;

import com.database.uokdb.SQLParser.DBObject.QueryColumn;
import com.database.uokdb.SQLParser.SQLOperationsTypes.CompareType;

/**
 * Created by Zloj on 21.11.2015.
 */
public class ConditionColumnToColumn implements Condition {
    private QueryColumn Column1;
    private QueryColumn ColumnToCompare;
    private CompareType CompareOperation;

    public ConditionColumnToColumn(QueryColumn Column1, QueryColumn ColumnToCompare, CompareType CompareOperation){
        this.Column1 = Column1;
        this.ColumnToCompare = ColumnToCompare;
        this.CompareOperation = CompareOperation;
    }


    public QueryColumn getColumn1() {
        return Column1;
    }

    public QueryColumn getColumnToCompare() {
        return ColumnToCompare;
    }

    public String getValueToCompare() {
        return null;
    }

    public CompareType getCompareOperation() {
        return CompareOperation;
    }
}
