package com.servlets;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by Alexey on 16.10.2015.
 */
@WebServlet("/signup")
public class RegisterServlet extends HttpServlet{

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html");
        RequestDispatcher dispatcher = request.getRequestDispatcher("/registration.jsp");
        dispatcher.forward(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String username = request.getParameter("username");
        String password = request.getParameter("password");
        String confirm = request.getParameter("confirm");
        String superpass = request.getParameter("superpass");
        if (!Administration.checkUsername(username)) {
            if (confirm.compareTo(password) == 0) {
                if(superpass.compareTo("") == 0) {
                    Administration.addUser(username, password, "user");
                    response.sendRedirect("/login");
                }
                else if(superpass.compareTo("123") == 0) {
                    Administration.addUser(username, password, "admin");
                    response.sendRedirect("/login");
                }
                else {
                    response.setContentType("text/html");
                    request.setAttribute("error", "Superpassword is not correct!");
                    RequestDispatcher dispatcher = request.getRequestDispatcher("/registration.jsp");
                    dispatcher.forward(request, response);
                }
            }
            else {
                response.setContentType("text/html");
                request.setAttribute("error", "Passwords are not the same!");
                RequestDispatcher dispatcher = request.getRequestDispatcher("/registration.jsp");
                dispatcher.forward(request, response);
            }
        }
        else {
            response.setContentType("text/html");
            request.setAttribute("error", "This username is already exists!");
            RequestDispatcher dispatcher = request.getRequestDispatcher("/registration.jsp");
            dispatcher.forward(request, response);
        }
    }
}
