package com.servlets;

import javax.servlet.http.Cookie;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Alexey on 29.10.2015.
 */
public class Administration {

    public static String name = "UOK_security";
    public static String value = "123";

    public static boolean checkPassword(String username, String password)
    {
        UserInfo.database.open();
        List<HashMap<String, Object>> result
                = UserInfo.database.query("select userid from users " +
                "where username = '" + username + "' and password = '" + password + "';");
        UserInfo.database.close();
        return result.size() > 0;
    }

    public static boolean checkUsername(String username){
        UserInfo.database.open();
        List<HashMap<String, Object>> result
                = UserInfo.database.query("select userid from users where username = '" + username + "';");
        UserInfo.database.close();
        return result.size() > 0;
    }

    public static boolean checkCookies(Cookie[] cookies) {
        if(cookies != null)
        for(Cookie c : cookies)
            if(c.getName().compareTo(name) == 0 && c.getValue().compareTo(value) == 0) {
                    return true;
            }
        return false;
    }

    public static String getCookieValue(Cookie[] cookies, String name){
        if(cookies != null)
            for(Cookie c : cookies)
                if(c.getName().compareTo(name) == 0) {
                    return c.getValue();
                }
        return null;
    }

    public static void addUser(String username, String password, String level){
        UserInfo.database.open();
        UserInfo.database.inputData("insert into users(username, password, level) values ('"+ username + "', '"
                + password + "', '" + level + "');");
        UserInfo.database.close();
    }

    public static String getUserLevel(String username){
        UserInfo.database.open();
        String level = (String) UserInfo.database.query("select level from users" +
                " where username = '" + username + "';").get(0).get("level");
        UserInfo.database.close();
        return level;
    }

}
