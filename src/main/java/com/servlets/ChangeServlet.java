package com.servlets;

import com.database.PostgreSQL;
import com.database.UOKDB;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by Alexey on 22.11.2015.
 */
@WebServlet("/change")
public class ChangeServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        if(UserInfo.database_name.compareTo("PostgreSQL") == 0) {
            UserInfo.database = new UOKDB();
            UserInfo.database_name = ("UOK DB");
        }
        else if(UserInfo.database_name.compareTo("UOK DB") == 0) {
            UserInfo.database = new PostgreSQL();
            UserInfo.database_name = ("PostgreSQL");
        }

        resp.sendRedirect("/login");
    }
}
