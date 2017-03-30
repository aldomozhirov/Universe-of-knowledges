package com.database;

import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

/**
 * Created by Alexey on 23.11.2015.
 */
public class Test1 {

    public static void main(String[] args) {
        DBinteraction database = new PostgreSQL();
        Scanner in = new Scanner(System.in);
        System.out.print("Query: ");
        String query = in.nextLine();
        database.open();
        List<HashMap<String, Object>> result = database.query(query);
        database.close();
        if (!result.isEmpty()) {
            System.out.print("|");
            for (String name : result.get(0).keySet())
                System.out.print(name + "|");
            System.out.println();
            for (HashMap<String, Object> tuple : result) {
                System.out.print("|");
                for (Object value : tuple.values())
                    System.out.print(value + "|");
                System.out.println();
            }
        }
    }

}
