package com.servlets;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by Alexey on 18.11.2015.
 */

@WebServlet("/insert")
public class InsertServlet extends HttpServlet {

    protected boolean checkInput(HttpServletRequest request, String... require){
        /*for(String key : require)
            if (request.getParameter(key).compareTo("") == 0)
                return false;*/
        return true;
    }

    protected void insertPublication(HttpServletRequest request)
    {
        UserInfo.database.open();

        UserInfo.database.inputData("insert into publication(title, year, area, type, url) values ('" +
                request.getParameter("title") + "', '" + request.getParameter("year") + "', '" +
                request.getParameter("area") + "', '" + request.getParameter("type") + "', '" +
                request.getParameter("url") + "');");

        int pubid = Integer.parseInt(UserInfo.database.query("select pubid from publication where " +
                "title = '" + request.getParameter("title") + "' and " + "year = '" +
                request.getParameter("year") + "';").get(0).get("pubid").toString());

        if(checkInput(request, "institution_name", "institution_country", "institution_city")) {
            UserInfo.database.inputData("insert into institution(institution_name, country, city) values ('" +
                    request.getParameter("institution_name") + "', '" + request.getParameter("institution_country")
                    + "', '" + request.getParameter("institution_city") + "');");

            int instid = Integer.parseInt(UserInfo.database.query("select instid from institution where " +
                    "institution_name = '" + request.getParameter("institution_name") + "';").get(0).get("instid").toString());

            UserInfo.database.inputData("insert into based_on(pubid, instid) values (" + pubid + ", " + instid + ");");
        }

        if(checkInput(request, "venue_name", "venue_type", "doi")) {
            UserInfo.database.inputData("insert into venue(venue_name, type, doi) values ('" +
                    request.getParameter("venue_name") + "', '" + request.getParameter("venue_type")
                    + "', '" + request.getParameter("doi") + "');");

            int venid = Integer.parseInt(UserInfo.database.query("select venid from venue where " +
                    "doi = '" + request.getParameter("doi") + "';").get(0).get("venid").toString());

            UserInfo.database.inputData("insert into published_on(pubid, venid) values (" + pubid + ", " + venid + ");");
        }

        if(checkInput(request, "event_name", "event_date")) {
            UserInfo.database.inputData("insert into event(event_name, edate) values ('" +
                    request.getParameter("event_name") + "', '" + request.getParameter("event_date") + "');");

            int evid = Integer.parseInt(UserInfo.database.query("select evid from event where " +
                    "event_name = '" + request.getParameter("event_name") + "';").get(0).get("evid").toString());

            UserInfo.database.inputData("insert into linked_with(pubid, evid) values (" + pubid + ", " + evid + ");");
        }

        if(checkInput(request, "keywords")) {
            String keywords = request.getParameter("keywords");
            for (String word : keywords.split("; ")) {
                UserInfo.database.inputData("insert into keyword(word) values ('" + word + "');");
                int keyid = Integer.parseInt(UserInfo.database.query("select keyid from keyword " +
                        "where word = '" + word + "';").get(0).get("keyid").toString());
                UserInfo.database.inputData("insert into has(pubid, keyid) values (" + pubid + ", " + keyid + ");");
            }
        }

        String authors = request.getParameter("authors");
        for(String author : authors.split("; ")) {
            UserInfo.database.inputData("insert into author(author_name) values ('" + author + "')");
            int autid = Integer.parseInt(UserInfo.database.query("select authid from author " +
                    "where author_name = '" + author + "';").get(0).get("authid").toString());
            UserInfo.database.inputData("insert into created_by(autid, pubid) values (" + autid + ", " + pubid + ");");
        }

        UserInfo.database.close();
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        RequestDispatcher dispatcher;
        if(request.getQueryString() ==  null) {
            dispatcher = request.getRequestDispatcher("/insert.jsp");
            response.setContentType("text/html");
            dispatcher.forward(request, response);
        }
        else if(checkInput(request, "title", "area", "type", "year", "url", "authors")) {
            insertPublication(request);
            response.sendRedirect("/search");
        }
        else{
            request.setAttribute("error", "Incorrect insert!");
            dispatcher = request.getRequestDispatcher("/insert.jsp");
            response.setContentType("text/html");
            dispatcher.forward(request, response);
        }

    }
}
