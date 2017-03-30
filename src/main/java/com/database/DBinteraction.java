package com.database;

import java.util.HashMap;
import java.util.List;

/**
 * Created by Alexey on 16.10.2015.
 */

public interface DBinteraction {

    void open();

    void close();

    void inputData(String SQL_query);

    List<HashMap<String, Object>> query(String SQL_query);

}
