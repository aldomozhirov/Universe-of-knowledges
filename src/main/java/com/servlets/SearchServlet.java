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
 * Created by Alexey on 20.10.2015.
 */
@WebServlet("/search")
public class SearchServlet extends HttpServlet {

    HttpServletRequest request;
    final static int RECORDS_ON_PAGE = 7;

    protected boolean checkEmpty(String... fieldNames)
    {
        for (String field : fieldNames)
        if (request.getParameter(field).compareTo("") != 0)
            return true;
        return false;
    }

    protected void doQuery(){
        QueryConstructor query = new QueryConstructor();

        if(checkEmpty("search", "title", "type", "area", "from_year", "to_year", "author_name", "institution_name",
                "institution_name", "institution_country", "institution_city", "venue_name",
                "venue_type", "doi", "event_name", "event_date")) {

            query.add_table("publication", "p");

            if (checkEmpty("title"))
                query.add_conditions("p.title = '" + request.getParameter("title") + "'");

            if (checkEmpty("type"))
                query.add_conditions("p.type = '" + request.getParameter("type") + "'");

            if (checkEmpty("area"))
                query.add_conditions("p.area = '" + request.getParameter("area") + "'");

            if (checkEmpty("from_year"))
                query.add_conditions("p.year >= '" + request.getParameter("from_year") + "'");

            if (checkEmpty("to_year"))
                query.add_conditions("p.year <= '" + request.getParameter("to_year") + "'");

            if (checkEmpty("search")) {
                query.add_table("keyword", "k");
                query.add_table("has", "h");
                query.add_conditions("k.keyid = h.keyid", "p.pubid = h.pubid",
                        "k.word = '" + request.getParameter("search") + "'");
            }

            if (checkEmpty("author_name")) {
                query.add_table("created_by", "c");
                query.add_table("author", "a");
                query.add_conditions("a.authid = c.autid", "p.pubid = c.pubid",
                        "a.author_name = '" + request.getParameter("author_name") + "'");
            }

            if (checkEmpty("institution_name", "institution_country", "institution_city")) {

                query.add_table("institution", "i");
                query.add_table("based_on", "bon");
                query.add_conditions("p.pubid = bon.pubid", "bon.instid = i.instid");

                if (checkEmpty("institution_name"))
                    query.add_conditions("i.institution_name = '" + request.getParameter("institution_name") + "'");

                if (checkEmpty("institution_country"))
                    query.add_conditions("i.country = '" + request.getParameter("institution_country") + "'");

                if (checkEmpty("institution_city"))
                    query.add_conditions("i.city = '" + request.getParameter("institution_city") + "'");
            }

            if (checkEmpty("venue_name", "venue_type", "doi")) {

                query.add_table("venue", "v");
                query.add_table("published_on", "pon");
                query.add_conditions("p.pubid = pon.pubid", "pon.venid = v.venid");

                if (checkEmpty("venue_name"))
                    query.add_conditions("v.venue_name = '" + request.getParameter("venue_name") + "'");

                if (checkEmpty("venue_type"))
                    query.add_conditions("v.type = '" + request.getParameter("venue_type") + "'");

                if (checkEmpty("doi"))
                    query.add_conditions("v.doi = '" + request.getParameter("doi") + "'");
            }

            if (checkEmpty("event_name", "event_date")) {

                query.add_table("event", "e");
                query.add_table("linked_with", "lw");
                query.add_conditions("p.pubid = lw.pubid", "lw.evid = e.evid");

                if (checkEmpty("event_name"))
                    query.add_conditions("e.event_name = '" + request.getParameter("event_name") + "'");

                if (checkEmpty("event_date"))
                    query.add_conditions("e.edate = '" + request.getParameter("event_date") + "'");

            }
            query.set_order(request.getParameter("sort") + " " + request.getParameter("order"));
            query.set_projection("p.pubid, p.title, p.year, p.type");
        }

        query.execute();

        UserInfo.result = query.getResult();
        UserInfo.pages = query.getResult().size() / RECORDS_ON_PAGE;
        UserInfo.query = request.getQueryString();
    }

    protected List<HashMap<String, Object>> getCurrentPart (){

        if(request.getParameter("button").compareTo("Search") == 0 ||
                request.getParameter("button").compareTo("<<") == 0)
            UserInfo.page = 0;
        else if(request.getParameter("button").compareTo(">") == 0) {
            if (UserInfo.page < UserInfo.pages)
                UserInfo.page++;
        }
        else if(request.getParameter("button").compareTo("<") == 0) {
            if (UserInfo.page > 0)
                UserInfo.page--;
        }
        else if(request.getParameter("button").compareTo(">>") == 0)
            UserInfo.page = UserInfo.pages;

        request.setAttribute("pages", UserInfo.pages);
        request.setAttribute("page", UserInfo.page);

        int from = UserInfo.page * RECORDS_ON_PAGE, to = UserInfo.page * RECORDS_ON_PAGE + RECORDS_ON_PAGE;
        if (to > UserInfo.result.size())
            to = UserInfo.result.size();
        return UserInfo.result.subList(from, to);
    }

    protected void setResults(List<HashMap<String, Object>> result){
        List<HashMap<String, Object>> output = new ArrayList<HashMap<String, Object>>();
        UserInfo.database.open();
        for(HashMap<String, Object> tuple : result) {
            HashMap<String, Object> record = new HashMap<String, Object>();
            record.put("id", tuple.get("pubid"));
            record.put("title", tuple.get("title") == null ? "Unknown publication title" : tuple.get("title"));
            record.put("year", tuple.get("year") == null ? "Unknown publication year" : tuple.get("year"));
            record.put("type", tuple.get("type") == null ? "Unknown publication type" : tuple.get("type"));

            List<HashMap<String, Object>> authorsQuery =
                    UserInfo.database.query("select a.author_name from author a, created_by c " +
                            "where c.autid = a.authid and c.pubid = " + tuple.get("pubid"));
            String authors;
            if(authorsQuery.isEmpty())
                authors = "Unknown author";
            else {
                Iterator<HashMap<String, Object>> iter = authorsQuery.iterator();
                authors = "";
                while (iter.hasNext()) {
                    authors += iter.next().get("author_name");
                    if (iter.hasNext())
                        authors += "; ";
                }
            }
            record.put("authors", authors);
            output.add(record);
        }
        request.setAttribute("data", output);
        //request.setAttribute("");

        UserInfo.database.close();
    }

    protected HttpServletRequest saveFields(HttpServletRequest request){
        if (request.getParameter("title").compareTo("") != 0 ||
            request.getParameter("area").compareTo("") != 0 ||
            request.getParameter("type").compareTo("") != 0 ||
            request.getParameter("from_year").compareTo("") != 0 ||
            request.getParameter("to_year").compareTo("") != 0)
                request.setAttribute("active0", "active");
        if (request.getParameter("author_name").compareTo("") != 0)
                request.setAttribute("active1", "active");
        if (request.getParameter("institution_name").compareTo("") != 0 ||
            request.getParameter("institution_country").compareTo("") != 0 ||
            request.getParameter("institution_city").compareTo("") != 0)
                request.setAttribute("active2", "active");
        if (request.getParameter("venue_name").compareTo("") != 0 ||
            request.getParameter("venue_type").compareTo("") != 0 ||
            request.getParameter("doi").compareTo("") != 0)
                request.setAttribute("active3", "active");
        if (request.getParameter("event_name").compareTo("") != 0 ||
            request.getParameter("event_date").compareTo("") != 0)
                request.setAttribute("active4", "active");

        return request;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        String username = Administration.getCookieValue(request.getCookies(), "UOK_username");
        boolean security = Administration.checkCookies(request.getCookies()) && Administration.checkUsername(username);

        if(username == null || !security) {
            response.sendRedirect("/login");
        }
        else {
            RequestDispatcher dispatcher;
            request.setAttribute("username", username);
            request.setAttribute("level", Administration.getUserLevel(username));
            this.request = request;
            if (request.getQueryString() == null) {
                dispatcher = request.getRequestDispatcher("/searchpage.jsp");
                response.setContentType("text/html");
                dispatcher.forward(request, response);
            } else {
                dispatcher = request.getRequestDispatcher("/resultpage.jsp");
                response.setContentType("text/html");
                if(request.getParameter("button").compareTo("Search") == 0)
                    doQuery();
                setResults(getCurrentPart());
                dispatcher.forward(saveFields(this.request), response);
            }
        }
    }

}
