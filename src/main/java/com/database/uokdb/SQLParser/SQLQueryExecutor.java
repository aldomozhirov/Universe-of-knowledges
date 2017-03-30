package com.database.uokdb.SQLParser;

import com.database.uokdb.SQLParser.DBObject.QueryColumn;
import com.database.uokdb.SQLParser.DBObject.QueryTable;
import com.database.uokdb.SQLParser.DBObject.QureyValue;
import com.database.uokdb.SQLParser.SQLConditions.Condition;
import com.database.uokdb.SQLParser.SQLConditions.ConditionColumnToValue;
import com.database.uokdb.db.DB;
import com.database.uokdb.dbusage.Table;


import java.util.HashMap;
import java.util.List;

/**
 * Created by Zloj on 22.11.2015.
 */
public class SQLQueryExecutor {
    String SQLText;
    SQLQuery Query;
    DB DataBase;

    public SQLQueryExecutor(String SQLText, DB Database){
        this.DataBase = Database;
        this.Query = new SQLQuery(SQLText);
    }

    private String[] columnLstToStringArray(List<QueryColumn> list){
        String[] temp = new String[list.size()];
        for(int i = 0; i < list.size(); i++){
            temp[i] = list.get(i).getName();
        }
        return temp;
    }

 public List<HashMap<String, Object>> executeSQL() {
        switch (Query.getQueryType()) {
            case SELECT:
                List<Condition> QueryConditions = Query.getCondition();
                List<QueryTable> From = Query.getTablesFrom();
                if (QueryConditions != null && QueryConditions.size() != 0) {
                    Condition MainCondition;
                    for (int i = 0; i < QueryConditions.size(); i++) {
                        if (QueryConditions.get(i) instanceof ConditionColumnToValue) {
                            MainCondition = QueryConditions.get(i);
                            QueryConditions.set(i, QueryConditions.get(0));
                            QueryConditions.set(i, MainCondition);
                            break;
                        }
                    }
                }
                Table ResultSet = tableByName(From.get(0).getName());
                From.remove(0);
                while (!From.isEmpty()) {
                    //ResultSet.joinWith(tableByName(From.get(0).getName()));
                    ResultSet.cartesian(tableByName(From.get(0).getName()));
                    From.remove(0);
                }

                ResultSet = TableAfterCondition(ResultSet, QueryConditions);
                String[] col = columnLstToStringArray(Query.getColumns());

                return ResultSet.project(col);
                //return ResultSet.getData();
            //break;

            case INSERT:
                Table InsertTable = tableByName(Query.getTablesToModify().get(0).getName());
                String[] InsertValues = insertValuesInOrder(Query.getValuesToSet());
                if (InsertValues[0] == null) InsertValues[0] = InsertTable.getNewId();
                InsertTable.insert(InsertValues);
                return null;
            //break;
            case UPDATE:
                Table UpdateTable = tableByName(Query.getTablesToModify().get(0).getName());
                UpdateTable = TableAfterCondition(UpdateTable, Query.getCondition());
                List<HashMap<String, Object>> Keys = UpdateTable.getData();
                //List<String> IDS = UpdateTable.sortBy(UpdateTable.getFieldNames()[0],true);
                String[] setData = insertValuesInOrder(Query.getValuesToSet());
                String keyFieldName = UpdateTable.getFieldNames()[0];
                for (HashMap<String, Object> row : Keys) {
                    setData[0] = row.get(keyFieldName).toString();
                    //UpdateTable.update(setData);
                    tableByName(Query.getTablesToModify().get(0).getName()).update(setData);
                }
                return null;
            //break;
            case DELETE:
                Table DeleteTable = tableByName(Query.getTablesToModify().get(0).getName());
                Table FromTable = tableByName(Query.getTablesFrom().get(0).getName());
                FromTable = TableAfterCondition(FromTable, Query.getCondition());
                List<HashMap<String, Object>> DeleteKeys = FromTable.project(FromTable.getFieldNames()[0]);
                //List<String> IDS = UpdateTable.sortBy(UpdateTable.getFieldNames()[0],true);
                //String[] setData = insertValuesInOrder(Query.getValuesToSet());
                String dKeyFieldName = DeleteTable.getFieldNames()[0];
                for (HashMap<String, Object> row : DeleteKeys) {
                    //setData[0] = row.get(keyFieldName);
                    DeleteTable.delete(dKeyFieldName, row.get(dKeyFieldName).toString());
                }
                return null;
        }
        return null;
    }

    private Table tableByName(String TableName){
        Table temp;
        String s = TableName.toUpperCase();
        if (s.equals("AUTHOR")) {
            temp = new Table(this.DataBase, "author", false, "authid", "author_name", "photo", "info");

        } else if (s.equals("BASED_ON")) {
            temp = new Table(this.DataBase, "based_on", false, "fakeID", "pubid", "instid");

        } else if (s.equals("CREATED_BY")) {
            temp = new Table(this.DataBase, "created_by", false, "fakeID", "autid", "pubid");

        } else if (s.equals("EVENT")) {
            temp = new Table(this.DataBase, "event", false, "evid", "event_name", "edate");

        } else if (s.equals("HAS")) {
            temp = new Table(this.DataBase, "has", false, "fakeID", "pubid", "keyid");

        } else if (s.equals("INSTITUTION")) {
            temp = new Table(this.DataBase, "institution", false, "instid", "institution_name", "country", "city");

        } else if (s.equals("KEYWORD")) {
            temp = new Table(this.DataBase, "keyword", false, "keyid", "word");

        } else if (s.equals("LINKED_WITH")) {
            temp = new Table(this.DataBase, "linked_with", false, "fakeID", "pubid", "evid");

        } else if (s.equals("PUBLICATION")) {
            temp = new Table(this.DataBase, "publication", false, "pubid", "title", "year", "area", "type", "url");

        } else if (s.equals("PUBLISHED_ON")) {
            temp = new Table(this.DataBase, "published_on", false, "fakeID", "pubid", "venid");

        } else if (s.equals("VENUE")) {
            temp = new Table(this.DataBase, "venue", false, "venid", "venue_name", "type", "doi");

        } else if (s.equals("USERS")) {
            temp = new Table(this.DataBase, "users", false, "userid", "username", "password", "level");

        } else {
            temp = null;
        }
        return temp;
    }

    private String[] insertValuesInOrder(List<QureyValue> values){
        int columnCount = tableByName(values.get(0).getColumn().getParentTable().getName()).getFieldNames().length;
        String[] temp = new String[columnCount];
        String s1 = values.get(0).getColumn().getParentTable().getName().toUpperCase();
        if (s1.equals("AUTHOR")) {
            for (QureyValue value : values) {
                String s = value.getColumn().getName().toLowerCase();
                if (s.equals("authid")) {
                    temp[0] = value.getValue();

                } else if (s.equals("author_name")) {
                    temp[1] = value.getValue();

                } else if (s.equals("photo")) {
                    temp[2] = value.getValue();

                } else if (s.equals("info")) {
                    temp[3] = value.getValue();

                }

            }

        } else if (s1.equals("BASED_ON")) {
            for (QureyValue value : values) {
                String s = value.getColumn().getName().toLowerCase();
                if (s.equals("pubid")) {
                    temp[0] = value.getValue();

                } else if (s.equals("instid")) {
                    temp[1] = value.getValue();

                }

            }

        } else if (s1.equals("CREATED_BY")) {
            for (QureyValue value : values) {
                String s = value.getColumn().getName().toLowerCase();
                if (s.equals("autid")) {
                    temp[0] = value.getValue();

                } else if (s.equals("pubid")) {
                    temp[1] = value.getValue();

                }
            }

        } else if (s1.equals("EVENT")) {
            for (QureyValue value : values) {
                String s = value.getColumn().getName().toLowerCase();
                if (s.equals("evid")) {
                    temp[0] = value.getValue();

                } else if (s.equals("event_name")) {
                    temp[1] = value.getValue();

                } else if (s.equals("edate")) {
                    temp[2] = value.getValue();

                }

            }

        } else if (s1.equals("HAS")) {
            for (QureyValue value : values) {
                String s = value.getColumn().getName().toLowerCase();
                if (s.equals("pubid")) {
                    temp[0] = value.getValue();

                } else if (s.equals("keyid")) {
                    temp[1] = value.getValue();

                }
            }

        } else if (s1.equals("INSTITUTION")) {
            for (QureyValue value : values) {
                String s = value.getColumn().getName().toLowerCase();
                if (s.equals("instid")) {
                    temp[0] = value.getValue();

                } else if (s.equals("institution_name")) {
                    temp[1] = value.getValue();

                } else if (s.equals("country")) {
                    temp[2] = value.getValue();

                } else if (s.equals("city")) {
                    temp[3] = value.getValue();

                }
            }

        } else if (s1.equals("KEYWORD")) {
            for (QureyValue value : values) {
                String s = value.getColumn().getName().toLowerCase();
                if (s.equals("keyid")) {
                    temp[0] = value.getValue();

                } else if (s.equals("word")) {
                    temp[1] = value.getValue();

                }
            }

        } else if (s1.equals("LINKED_WITH")) {
            for (QureyValue value : values) {
                String s = value.getColumn().getName().toLowerCase();
                if (s.equals("pubid")) {
                    temp[0] = value.getValue();

                } else if (s.equals("evid")) {
                    temp[1] = value.getValue();

                }
            }

        } else if (s1.equals("PUBLICATION")) {
            for (QureyValue value : values) {
                String s = value.getColumn().getName().toLowerCase();
                if (s.equals("pubid")) {
                    temp[0] = value.getValue();

                } else if (s.equals("title")) {
                    temp[1] = value.getValue();

                } else if (s.equals("year")) {
                    temp[2] = value.getValue();

                } else if (s.equals("area")) {
                    temp[3] = value.getValue();

                } else if (s.equals("type")) {
                    temp[4] = value.getValue();

                } else if (s.equals("url")) {
                    temp[5] = value.getValue();

                }
            }

        } else if (s1.equals("PUBLISHED_ON")) {
            for (QureyValue value : values) {
                String s = value.getColumn().getName().toLowerCase();
                if (s.equals("pubid")) {
                    temp[0] = value.getValue();

                } else if (s.equals("venid")) {
                    temp[1] = value.getValue();

                }
            }

        } else if (s1.equals("VENUE")) {
            for (QureyValue value : values) {
                String s = value.getColumn().getName().toLowerCase();
                if (s.equals("venid")) {
                    temp[0] = value.getValue();

                } else if (s.equals("venue_name")) {
                    temp[1] = value.getValue();

                } else if (s.equals("type")) {
                    temp[2] = value.getValue();

                } else if (s.equals("doi")) {
                    temp[3] = value.getValue();

                }
            }

        } else if (s1.equals("USERS")) {
            for (QureyValue value : values) {
                String s = value.getColumn().getName().toLowerCase();
                if (s.equals("userid")) {
                    temp[0] = value.getValue();

                } else if (s.equals("username")) {
                    temp[1] = value.getValue();

                } else if (s.equals("password")) {
                    temp[2] = value.getValue();

                } else if (s.equals("level")) {
                    temp[3] = value.getValue();

                }
            }

        }
        return temp;
    }

    private Table TableAfterCondition(Table table, Condition condition) {
        Table temp = null;
        String[] ColumnNames = table.getFieldNames();
        if (!ColumnNames[0].contains("\\.")) {
            if (condition instanceof ConditionColumnToValue) {
                String condColumnFullName = condition.getColumn1().getName().toLowerCase();
                for (String colname : ColumnNames) {
                    if (condColumnFullName.equals(colname.toLowerCase())) {
                        temp = table.select(condition.getColumn1().getName().toLowerCase(), condition.getValueToCompare(), condition.getCompareOperation());
                        return temp;
                    }
                }
            } else {
                String condColumn1FullName = condition.getColumn1().getName().toLowerCase();
                String condColumn2FullName = condition.getColumnToCompare().getName().toLowerCase();
                for (String colname : ColumnNames) {
                    if (condColumn1FullName.equals(colname.toLowerCase())) {
                        for (String colname2 : ColumnNames) {
                            if (condColumn2FullName.equals(colname2.toLowerCase())) {
                                temp = table.selectWhere(condColumn1FullName, condColumn2FullName, condition.getCompareOperation());
                                return temp;
                            }
                        }
                    }
                }
            }
        } else {
            for (int i = 0; i < ColumnNames.length; i++) {
                if (!ColumnNames[i].contains("\\.")) ColumnNames[i] = table.getName() + "." + ColumnNames[i];
            }
            if (condition instanceof ConditionColumnToValue) {
                String condColumnFullName = (condition.getColumn1().getParentTable().getName() + "." + condition.getColumn1().getName()).toLowerCase();
                for (String colname : ColumnNames) {
                    if (condColumnFullName.equals(colname.toLowerCase())) {
                        temp = table.select(condition.getColumn1().getName().toLowerCase(), condition.getValueToCompare(), condition.getCompareOperation());
                        return temp;
                    }
                }
            } else {
                String condColumn1FullName = (condition.getColumn1().getParentTable().getName() + "." + condition.getColumn1().getName()).toLowerCase();
                String condColumn2FullName = (condition.getColumnToCompare().getParentTable().getName() + "." + condition.getColumnToCompare().getName()).toLowerCase();
                for (String colname : ColumnNames) {
                    if (condColumn1FullName.equals(colname.toLowerCase())) {
                        for (String colname2 : ColumnNames) {
                            if (condColumn2FullName.equals(colname.toLowerCase())) {
                                temp = table.selectWhere(condColumn1FullName, condColumn2FullName, condition.getCompareOperation());
                                return temp;
                            }
                        }
                    }
                }
            }
        }
        return table;
    }

    private Table TableAfterCondition(Table table, List<Condition> conditions){
        Table result = table;
        for(Condition cond:conditions){
            Table temp = TableAfterCondition(table, cond);
            result = (temp==null? result: temp);
        }
        return result;
    }

}
