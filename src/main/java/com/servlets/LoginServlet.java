package com.servlets;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by Alexey on 15.10.2015.
 */
@WebServlet("/login")
public class LoginServlet extends HttpServlet {

    Administration admin = new Administration();

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        if(Administration.checkCookies(request.getCookies())) {
            response.sendRedirect("/search");
        }
        else {
            RequestDispatcher dispatcher = request.getRequestDispatcher("/login.jsp");
            request.setAttribute("dbms", UserInfo.database_name);
            response.setContentType("text/html");
            dispatcher.forward(request, response);
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException{

        String username = request.getParameter("username");
        String password = request.getParameter("password");

        if(Administration.checkPassword(username, password)) {
            response.addCookie(new Cookie("UOK_security", "123"));
            response.addCookie(new Cookie("UOK_username", username));
            response.sendRedirect("/search");
        }
        else {
            request.setAttribute("dbms", UserInfo.database_name);
            request.setAttribute("error", "Incorrect username/password!");
            RequestDispatcher dispatcher = request.getRequestDispatcher("/login.jsp");
            response.setContentType("text/html");
            dispatcher.forward(request, response);
        }
    }

}
