package com.database.uokdb.SQLParser.SQLConditions;

import com.database.uokdb.SQLParser.DBObject.QueryColumn;
import com.database.uokdb.SQLParser.SQLOperationsTypes.CompareType;

/**
 * Inteface describes all types of SQL conditions:
 * QueryColumn compares to column,
 * QueryColumn compares to value
 *
 * Created by Zloj on 21.11.2015.
 */
public interface Condition {
    public QueryColumn getColumn1();
    public QueryColumn getColumnToCompare();
    public String getValueToCompare();
    public CompareType getCompareOperation();
}
