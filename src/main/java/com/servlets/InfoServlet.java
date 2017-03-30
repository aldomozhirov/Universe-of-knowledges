package com.servlets;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

/**
 * Created by Alexey on 09.11.2015.
 */
@WebServlet("/info")
public class InfoServlet extends HttpServlet {

    protected HashMap<String, Object> getInformation(String id){
        HashMap<String, Object> information = new HashMap<String, Object>();
        List<HashMap<String, Object>> result;

        UserInfo.database.open();

        result = UserInfo.database.query("select pubid, title, area, year, url from publication where pubid = " + id + ";");
        if(!result.isEmpty()) {
            information.put("title", result.get(0).get("title") == null ?
                    null : result.get(0).get("title"));
            information.put("area", result.get(0).get("area") == null ?
                    null : result.get(0).get("area"));
            information.put("year", result.get(0).get("year") == null ?
                    null : result.get(0).get("year"));
            information.put("type", result.get(0).get("type") == null ?
                    null : result.get(0).get("type"));
            information.put("url", result.get(0).get("url") == null ?
                    null : result.get(0).get("url"));
        }

        result = UserInfo.database.query("select a.author_name from author a, created_by c " +
                "where c.autid = a.authid and c.pubid = " + id + ";");
        if(!result.isEmpty()) {
            String authors;
            Iterator<HashMap<String, Object>> iter = result.iterator();
            authors = "";
            while (iter.hasNext()) {
                authors += iter.next().get("author_name");
                if (iter.hasNext())
                    authors += "; ";
            }
            information.put("authors", authors == null ? null : authors);
        }

        result = UserInfo.database.query("select i.institution_name, i.country, i.city from publication p, based_on b, institution i " +
                "where p.pubid = b.pubid and b.instid = i.instid and p.pubid = " + id + ";");

        if(!result.isEmpty()) {
            information.put("institution_name", result.get(0).get("institution_name") == null ?
                    null : result.get(0).get("institution_name"));
            information.put("institution_country", result.get(0).get("country") == null ?
                    null : result.get(0).get("country"));
            information.put("institution_city", result.get(0).get("city") == null ?
                    null : result.get(0).get("city"));
        }

        result = UserInfo.database.query("select v.venue_name, v.type, v.doi from publication p, published_on o, venue v " +
                "where p.pubid = o.pubid and o.venid = v.venid and p.pubid = " + id + ";");

        if (!result.isEmpty()) {
            information.put("venue_name", result.get(0).get("venue_name") == null ?
                    null : result.get(0).get("venue_name"));
            information.put("venue_type", result.get(0).get("type") == null ?
                    null : result.get(0).get("type"));
            information.put("doi", result.get(0).get("doi") == null ?
                    null : result.get(0).get("doi"));
        }

        result = UserInfo.database.query("select e.event_name, e.edate from publication p, linked_with l, event e where " +
                "p.pubid = l.pubid and l.evid = e.evid and p.pubid = " + id + ";");

        if(!result.isEmpty()) {
            information.put("event_name", result.get(0).get("event_name") == null ?
                    null : result.get(0).get("event_name"));
            information.put("event_date", result.get(0).get("edate") == null ?
                    null : result.get(0).get("edate"));
        }

        result = UserInfo.database.query("select k.word from keyword k, has h " +
                "where h.keyid = k.keyid and h.pubid = " + id + ";");
        if(!result.isEmpty()) {
            String keywords;
            Iterator<HashMap<String, Object>> iter = result.iterator();
            keywords = "";
            while (iter.hasNext()) {
                keywords += iter.next().get("word");
                if (iter.hasNext())
                    keywords += "; ";
            }
            information.put("keywords", keywords == null ? null : keywords);
        }

        UserInfo.database.close();
        return information;
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String username = Administration.getCookieValue(request.getCookies(), "UOK_username");
        boolean security = Administration.checkCookies(request.getCookies()) && Administration.checkUsername(username);

        if(username == null || !security) {
            response.sendRedirect("/login");
        }
        else {
            RequestDispatcher dispatcher;
            request.setAttribute("username", username);
            request.setAttribute("level", Administration.getUserLevel(username));
            request.setAttribute("information", getInformation(request.getParameter("id")));
            request.setAttribute("back", "?" + UserInfo.query);
            if(request.getParameter("type").compareTo("update") == 0 &&
                    Administration.getUserLevel(username).compareTo("admin") != 0)
                response.sendRedirect(request.getRequestURI());
            else {
                dispatcher = request.getRequestDispatcher("/info.jsp");
                response.setContentType("text/html");
                dispatcher.forward(request, response);
            }
        }
    }
}
