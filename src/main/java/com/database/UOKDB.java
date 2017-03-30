package com.database;

import com.database.uokdb.SQLParser.SQLQueryExecutor;
import com.database.uokdb.db.DB;
import com.database.uokdb.db.DBMaker;
import com.database.uokdb.dbusage.Table;

import java.io.File;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Alexey on 22.11.2015.
 */
public class UOKDB implements DBinteraction {

    DB db = null;

    protected void createTables() {
        new Table(db, "users", true, "userid", "username", "password", "levels");
        new Table(db, "publication", true, "pubid", "title", "year", "area", "type", "url");
        new Table(db, "author", true, "authid", "author_name", "photo", "info");
        new Table(db, "institution", true, "instid", "institiution_name", "country", "city");
        new Table(db, "venue", true, "venid", "venue_name", "type", "doi");
        new Table(db, "keyword", true, "keyid", "word");
        new Table(db, "event", true, "evid", "event_name", "edate");
        new Table(db, "created_by", true, "autid", "pubid");
        new Table(db, "based_on", true, "pubid", "instid");
        new Table(db, "published_on", true, "pubid", "venid");
        new Table(db, "has", true, "pubid", "keyid");
        new Table(db, "linked_with", true, "pubid", "evid");
    }

    public void open() {
        db = DBMaker.fileDB(new File("C:\\Users\\Alexey\\IdeaProjects\\Maven Project\\db")).cacheSize(2048).closeOnJvmShutdown().make();
        createTables();
    }

    public void close() {
        db.close();
    }

    public void inputData(String SQL_query) {
        SQLQueryExecutor executor = new SQLQueryExecutor(SQL_query, db);
        executor.executeSQL();
    }

    public List<HashMap<String, Object>> query(String SQL_query) {
        SQLQueryExecutor executor = new SQLQueryExecutor(SQL_query, db);
        List l = executor.executeSQL();
        return  l;
    }

}
