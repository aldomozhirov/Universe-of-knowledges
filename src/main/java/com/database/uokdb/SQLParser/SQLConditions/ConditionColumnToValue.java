package com.database.uokdb.SQLParser.SQLConditions;

import com.database.uokdb.SQLParser.DBObject.QueryColumn;
import com.database.uokdb.SQLParser.SQLOperationsTypes.CompareType;

/**
 * Created by Zloj on 21.11.2015.
 */
public class ConditionColumnToValue implements Condition{

    private QueryColumn Column1;
    private String ValueToCompare;
    private CompareType CompareOperation;

    public ConditionColumnToValue(QueryColumn Column1, String ValueToCompare, CompareType CompareOperation){
        this.Column1 = Column1;
        this.ValueToCompare = ValueToCompare;
        this.CompareOperation = CompareOperation;
    }

    public QueryColumn getColumn1() {
        return Column1;
    }

    public QueryColumn getColumnToCompare() {
        return null;
    }

    public String getValueToCompare() {
        return ValueToCompare;
    }

    public CompareType getCompareOperation() {
        return CompareOperation;
    }
}
