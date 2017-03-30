package com.servlets;

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
@WebServlet("/logout")
public class LogoutServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        doPost(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        request.getSession().invalidate();
        Cookie[] c = request.getCookies();
        for(Cookie cur : c)
            if(cur.getName().compareTo("UOK_security") == 0 || cur.getName().compareTo("UOK_username") == 0) {
                cur.setMaxAge(0);
                response.addCookie(cur);
            }
        response.sendRedirect("/login");
    }
}
