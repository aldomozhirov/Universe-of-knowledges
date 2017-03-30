package com.servlets;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by Alexey on 19.11.2015.
 */
@WebServlet("/update")
public class UpdateServlet extends HttpServlet {

    protected String correction(String parameter){
        if(parameter.compareTo("") == 0)
            return null;
        else
            return "'" + parameter + "'";
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        UserInfo.database.open();

        UserInfo.database.inputData("update publication set title = " + correction(request.getParameter("title")) +
        ", type = " + correction(request.getParameter("type")) + ", year = " + correction(request.getParameter("year")) +
                " where pubid = " + request.getParameter("id") + ";");

        UserInfo.database.close();
        response.sendRedirect("/info?id=" + request.getParameter("id") + "&type=show");
    }
}
