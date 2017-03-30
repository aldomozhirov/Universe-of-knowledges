package com.database;

import com.database.DBinteraction;
import com.database.PostgreSQL;
import com.database.UOKDB;
import com.database.uokdb.SQLParser.SQLQueryExecutor;
import com.database.uokdb.db.DB;
import com.database.uokdb.db.DBMaker;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * Created by Alexey on 23.11.2015.
 */
public class Reparser {

    public static void main(String[] args) throws FileNotFoundException {
        DBinteraction database = new UOKDB();
        Scanner scanner = new Scanner(new File("data.sql"));
        database.open();
        System.out.println("Start");
        while (scanner.hasNextLine()) {
            database.inputData(scanner.nextLine());
        }
        database.close();
        System.out.println("Done!");
        scanner.close();
    }

}
