package com.servlets;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by Alexey on 18.11.2015.
 */

@WebServlet("/delete")
public class DeleteServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String id = request.getParameter("id");
        UserInfo.database.open();
        UserInfo.database.inputData("delete from created_by where pubid = " + id + ";");
        UserInfo.database.inputData("delete from linked_with where pubid = " + id + ";");
        UserInfo.database.inputData("delete from published_on where pubid = " + id + ";");
        UserInfo.database.inputData("delete from has where pubid = " + id);
        UserInfo.database.inputData("delete from publication where pubid = " + id + ";");
        UserInfo.database.close();
        response.sendRedirect("/search?" + UserInfo.query);
    }
}
