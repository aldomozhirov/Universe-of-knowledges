package com.database.uokdb.SQLParser;

import com.database.uokdb.SQLParser.DBObject.QueryColumn;
import com.database.uokdb.SQLParser.DBObject.QueryTable;
import com.database.uokdb.SQLParser.DBObject.QureyValue;
import com.database.uokdb.SQLParser.SQLConditions.Condition;
import com.database.uokdb.SQLParser.SQLConditions.ConditionColumnToColumn;
import com.database.uokdb.SQLParser.SQLConditions.ConditionColumnToValue;
import com.database.uokdb.SQLParser.SQLOperationsTypes.CompareType;
import com.database.uokdb.SQLParser.SQLOperationsTypes.SQLQueryType;

import java.util.*;

/**
 * Created by Zloj on 20.11.2015.
 */
public class SQLQuery {
    private SQLQueryType QueryType;
    List<QueryTable> TablesFrom;
    List<QueryColumn> Columns;
    List<Condition> Condition;
    List<QueryTable> TablesToModify;
    List<QureyValue> ValuesToSet;
    List<QueryColumn> GroupByFields;
    boolean Distinct;
    int Limit;


    List<String> ReservedWords  = new ArrayList<String>(Arrays.asList("FROM", "WHERE", "GROUP BY", "ORDER BY" ,"DISTINCT","LIMIT",";"));

    public SQLQuery(String SQL){
        TablesFrom = new ArrayList<QueryTable>();
        Columns = new ArrayList();
        Condition = new ArrayList();
        TablesToModify = new ArrayList();
        ValuesToSet = new ArrayList();
        GroupByFields = new ArrayList();
        Distinct = false;
        Limit = -1;
        parse3(SQL);
    }

    //region Getters
    public SQLQueryType getQueryType(){
        return QueryType;
    }
    public List getTablesFrom(){
        return TablesFrom;
    }
    public List getColumns(){
        return Columns;
    }
    public List<Condition> getCondition() {
        return this.Condition;
    }
    public List<QueryTable> getTablesToModify() {
        return this.TablesToModify;
    }
    public List<QureyValue> getValuesToSet() {
        return ValuesToSet;
    }
    public List<QueryColumn> getGroupByFields(){
        return this.GroupByFields;
    }
    public boolean getDistinct(){
        return Distinct;
    }
    public int getLimit() {
        return Limit;
    }
    //endregion

    private void parse2(String sql){
        sql = sql.trim().toUpperCase();
        int endHead = posOfReservedWord(sql);
        String Head = sql.substring(0, endHead);
        int posFrom = sql.indexOf("FROM");
        //ПАРСИМ FROM если есть
        if (posFrom!=-1) {
            int endPosFrom = posFrom + 4 + posOfReservedWord(sql.substring(posFrom + 4));
            String From = sql.substring(posFrom + 4, endPosFrom);
            int posCondition = sql.indexOf("WHERE");
            String Condition = sql.substring(posCondition+5);
            parseFrom(From);
            parseCondition(Condition);
        }
        parseHead(Head);
    }

    private void parse3(String sql){
        sql = sql.trim();
        int posDistinct = sql.toUpperCase().indexOf("DISTINCT");
        if (posDistinct > -1){
            sql = sql.substring(0,posDistinct) + sql.substring(posDistinct + 8);
            this.Distinct = true;
        }
        switch(parseType(sql)){
            case SELECT:
                parseSelect(sql);
                break;
            case INSERT:
                parseInsert(sql);
                break;
            case UPDATE:
                parseUpdate(sql);
                break;
            case DELETE:
                parseDelete(sql);
                break;
        }

    }

    private void parseUpdate(String sql) {
        int posTable = sql.toUpperCase().indexOf("UPDATE")+6;                             //парсим название таблицы
        int endTable = sql.indexOf("SET");
        String Head = sql.substring(posTable, endTable).trim();
        this.TablesToModify.add(new QueryTable(Head));
        int endSet = posOfReservedWord(sql);
        String setString = sql.substring(endTable+3, endSet);

        String[] setUnits = setString.split(",");                                         //парсим колонки и значения
        for(String setUnit: setUnits){
            String[] splittedUnit = setUnit.split("=");
            QueryColumn setColumn = new QueryColumn(splittedUnit[0].trim());
            setColumn.setParentTable(TablesToModify.get(0));
            QureyValue setValue = new QureyValue(setColumn, splittedUnit[1].trim().replace("'",""));
            this.Columns.add(setColumn);
            this.ValuesToSet.add(setValue);
        }
        int posCondition = sql.toUpperCase().indexOf("WHERE");
        if (posCondition > -1) {
            String Condition = sql.substring(posCondition + 5);
            parseCondition(Condition);
        }
    }

    private void parseInsert(String sql) {
        int posTable = sql.toUpperCase().indexOf("INTO")+4;                             //парсим название таблицы
        int endTable = sql.indexOf("(");
        String Head = sql.substring(posTable, endTable).trim();
        this.TablesToModify.add(new QueryTable(Head));
        int endColumns = sql.indexOf(")");                              //парсим названия колонок
        String[] columns = sql.substring(endTable+1, endColumns).split(",");
        for(String column:columns){
            column = column.trim();
            QueryColumn ColumnToAdd = new QueryColumn(column);
            ColumnToAdd.setParentTable(this.TablesToModify.get(0));
            this.Columns.add(ColumnToAdd);
        }
        sql = sql.substring(sql.toUpperCase().indexOf("VALUES")+6);                   //ОБРЕЗАЕМ СТРОКУ sql!!!
        int posValues = sql.indexOf("(")+1;
        int endValues = sql.indexOf(")");
        String tempValues = sql.substring(posValues, endValues);

        for(int k = 0; k < tempValues.length(); k++){
            if(tempValues.charAt(k) == '\''){
                k++;
                while (k < tempValues.length()) {
                    if(tempValues.charAt(k) == ','){
                        tempValues = tempValues.substring(0,k)+ "?" + tempValues.substring(k+1,tempValues.length());
                    }
                    if(tempValues.charAt(k) == '\''){
                        break;
                    }
                    k++;
                }
            }
        }
        String[] values = tempValues.split(",");
        for(int i = 0; i < values.length; i++){
            values[i] = values[i].trim();           //удаляем пробелы
            values[i] = values[i].replace("'","");  //удаляем кавычки
            values[i] = values[i].replace("?","");  //удаляем условный разделитель
            QureyValue ValueToAdd = new QureyValue(this.Columns.get(i), values[i]);
            this.ValuesToSet.add(ValueToAdd);
        }
    }

    private void parseDelete(String sql) {
        int endHead = posOfReservedWord(sql);
        String Head = sql.substring(0, endHead);
        int posFrom = sql.toUpperCase().indexOf("FROM");
        //ПАРСИМ FROM если есть
        if (posFrom!=-1) {
            int endPosFrom = posFrom + 4 + posOfReservedWord(sql.substring(posFrom + 4));
            String From = sql.substring(posFrom + 4, endPosFrom);
            int posCondition = sql.toUpperCase().indexOf("WHERE");
            String Condition = sql.substring(posCondition+5);
            parseFrom(From);
            parseCondition(Condition);
        }
        parseDeleteHead(Head);
    }

    private void parseSelect(String sql) {
        int endHead = posOfReservedWord(sql);
        String Head = sql.substring(0, endHead);
        int posFrom = sql.toUpperCase().indexOf("FROM");
        //ПАРСИМ FROM если есть
        if (posFrom!=-1) {
            int endPosFrom = posFrom + 4 + posOfReservedWord(sql.substring(posFrom + 4));
            String From = sql.substring(posFrom + 4, endPosFrom);

            parseFrom(From);
            int posCondition = sql.toUpperCase().indexOf("WHERE");
            int posGroupBy = sql.toUpperCase().indexOf("GROUP BY");
            int posLimit = sql.toUpperCase().indexOf("LIMIT");
            if (posCondition > -1) {
                String Condition = sql.substring(posCondition + 5);
                int endCondition = posOfReservedWord(Condition);
                if (endCondition > 0) Condition = Condition.substring(0, endCondition);
                parseCondition(Condition);
            }

            if (posGroupBy > -1) {
                String groupBy = sql.substring(posGroupBy + 8);
                int endGroupBy = posOfReservedWord(groupBy);
                if (endGroupBy > 0) groupBy = groupBy.substring(0, endGroupBy);
                parseGroupBy(groupBy);
            }

            if (posLimit > -1) {
                String limit = sql.substring(posLimit + 5);
                int endLimit = posOfReservedWord(limit);
                if (endLimit > 0) limit = limit.substring(0, endLimit);
                limit = limit.trim();
                int limitCount;
                try {
                    limitCount = Integer.parseInt(limit);
                }catch(NumberFormatException e){
                    limitCount = -1;
                }
                this.Limit = limitCount;
            }
        }
        parseHead(Head);
    }

    private void parseCondition(String condition) {
        int endCondition = posOfReservedWord(condition);
        if (endCondition > 0) condition = condition.substring(0, endCondition);
        condition = condition.trim();
        String[] UnparsedConditions = condition.split("(?i)AND"); //TODO: реализовать алгоритм разбиения по всем логическим операциям!!!
        for(String UnparsedCondition: UnparsedConditions){
            String[] compareOperators = {">=","<=","!=",">","<","="};
            UnparsedCondition = UnparsedCondition.trim();
            int operatorCode = -1;
            int operatorPos = -1;
            for(int i = 0;i <compareOperators.length; i++){                 //поиск оператора сравнения в строке
                int pos = UnparsedCondition.indexOf(compareOperators[i]);
                if (pos > -1){
                    operatorCode = i;
                    operatorPos = pos;
                    break;
                }
            }

            CompareType operator;
            switch(operatorCode){                                           //исходя из найденного оператора парсим
                case 0: operator = CompareType.MoreEq; break;
                case 1: operator = CompareType.LessEq; break;
                case 2: operator = CompareType.NotEq; break;
                case 3: operator = CompareType.More; break;
                case 4: operator = CompareType.Less; break;
                case 5: operator = CompareType.Eq; break;
                default:continue;
            }
            String comp1 = UnparsedCondition.substring(0, operatorPos).trim();
            String comp2 = UnparsedCondition.substring(operatorPos+compareOperators[operatorCode].length()).trim();
            QueryColumn col1 = parseColumn(comp1);
            String value;
            QueryColumn col2;
            Condition cond;
            if (comp2.matches("'.*'")){
                value = comp2.replace("'", "");
                cond = new ConditionColumnToValue(col1, value, operator);
            }else{
                col2 = parseColumn(comp2);
                cond = new ConditionColumnToColumn(col1, col2, operator);
            }
            this.Condition.add(cond);
        }
    }

    private void parseHead(String head) {
        //query type parsing
        StringTokenizer st = new StringTokenizer(head,  " \t\n\r,");
        String type = st.nextToken();
        parseType(type);
        //columns parsing
        head = head.replace(type, "");
        String[] Columns = head.split(",");
        for(String UnparsedColumn:Columns){
            this.Columns.add(parseColumn(UnparsedColumn));
        }
    }

    private void parseDeleteHead(String head) {
        //query type parsing
        StringTokenizer st = new StringTokenizer(head,  " \t\n\r,");
        String type = st.nextToken();
        //parseType(type);
        //columns parsing
        head = head.replace(type, "").trim();
        String[] Columns = head.split(",");
        if (Columns.length == 1 && Columns[0].length() == 0) {
            this.TablesToModify = this.TablesFrom;
        } else {
            for (String UnparsedColumn : Columns) {
                this.TablesToModify.add(tableByAlias(UnparsedColumn.trim()));
            }
        }
    }

    //TODO: разобраться с алгоритмом инициализации this.QueryType
    private SQLQueryType parseType(String s) {
        s = s.toUpperCase();
        //if(s == SQLParser.SQLOperationsTypes.SQLQueryType.valueOf(s).name()) QueryType = SQLParser.SQLOperationsTypes.SQLQueryType.valueOf(s);
        s = s.trim().substring(0,6);
        if (s.equals("SELECT")) {
            QueryType = SQLQueryType.SELECT;
            return SQLQueryType.SELECT;
        } else if (s.equals("INSERT")) {
            QueryType = SQLQueryType.INSERT;
            return SQLQueryType.INSERT;
        } else if (s.equals("UPDATE")) {
            QueryType = SQLQueryType.UPDATE;
            return SQLQueryType.UPDATE;
        } else if (s.equals("DELETE")) {
            QueryType = SQLQueryType.DELETE;
            return SQLQueryType.DELETE;
        } else {
            System.out.println("Impossible to parse '" + s + "'");
            return null;
        }
    }

    private void parseFrom(String s){
        String[] TablesNames = s.split(",");
        for(String TableName:TablesNames){
            String[] NameAndAlias = TableName.trim().split(" ");
            QueryTable table;
            switch(NameAndAlias.length){
                case 1:
                    table = new QueryTable(NameAndAlias[0], NameAndAlias[0]);
                    break;
                case 2:
                    table = new QueryTable(NameAndAlias[0], NameAndAlias[1]);
                    break;
                case 3:
                    if(NameAndAlias[1].toUpperCase().equals("AS")) table = new QueryTable(NameAndAlias[0], NameAndAlias[2]);
                    else table = new QueryTable(NameAndAlias[0], NameAndAlias[0]);
                    break;
                default: table = new QueryTable(NameAndAlias[0], NameAndAlias[0]); break;
            }
            //QueryTable table = new QueryTable(NameAndAlias[0],NameAndAlias.length >= 2? NameAndAlias[1]:NameAndAlias[0]);
            this.TablesFrom.add(table);
        }
    }

    /**
     * Возможно имеет смысл перенести метод в конструктор класса QueryColumn
     * @param UnparsedColumnName - string with table name, column name and alias
     * @return
     */
    private QueryColumn parseColumn(String UnparsedColumnName){
        String[] NameAndAlias = UnparsedColumnName.trim().split(" ");
        String[] TableAndColumn = NameAndAlias[0].split("\\.");
        QueryColumn column;
        if(TableAndColumn.length == 1) {
            column = new QueryColumn(TableAndColumn[0], NameAndAlias.length >= 2 ? NameAndAlias[1] : NameAndAlias[0]);
            QueryTable parent = tableByAlias(TableAndColumn[0]);
            if (parent != null) column.setParentTable(parent);
            else{
                if (this.TablesFrom.size() == 1) column.setParentTable(TablesFrom.get(0));
            }
        }else{
            column = new QueryColumn(TableAndColumn[1], NameAndAlias.length >= 2 ? NameAndAlias[1] : TableAndColumn[1]);
            QueryTable parent = tableByAlias(TableAndColumn[0]);
            if (parent != null) column.setParentTable(parent);
            else{
                if (this.TablesFrom.size() == 1) column.setParentTable(TablesFrom.get(0));
            }
        }
        return column;
    }

    private void parseGroupBy(String sql){
        int posGroupBy = sql.toUpperCase().indexOf("GROUP BY");
        if (posGroupBy != -1) sql = sql.substring(posGroupBy + 8);
        int endGroupBy = posOfReservedWord(sql);
        if (endGroupBy != -1) sql= sql.substring(0, endGroupBy);
        String[] columnsGB = sql.split(",");
        for(String unparsedColumn: columnsGB){
            GroupByFields.add(parseColumn(unparsedColumn));
        }
    }

    private int posOfReservedWord(String str){
        str = str.toUpperCase();
        int pos = -1;
        for(String Word:ReservedWords){
            int tempPos = str.indexOf(Word);
            if (tempPos > -1) {
                if (pos == -1) pos = tempPos;
                pos = Math.min(pos, tempPos);
            }
        }
        return pos;
    }

    /**
     * Returns QueryTable that has needed alias
     * @param Alias
     * @return QueryTable
     */
    private QueryTable tableByAlias(String Alias){
        for(QueryTable table: TablesFrom){
            if (table.getAlias().equals(Alias)) return table;
        }
        return null;
    }

}
