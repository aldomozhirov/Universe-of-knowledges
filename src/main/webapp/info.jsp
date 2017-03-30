<%@ page import="java.util.HashMap" %>
<%--
  Created by IntelliJ IDEA.
  User: Alexey
  Date: 09.11.2015
  Time: 23:05
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Universe of knowledges</title>
  <link rel="stylesheet" type="text/css" href="css/semantic.min.css">
  <link href="css/styles.css" rel="stylesheet" type="text/css">
  <script src="js/jquery-2.1.4.min.js"></script>
  <script src="js/semantic.min.js"></script>
</head>
<body>
<div class="bar">
  <a class="ui inverted blue button right" href="/logout">Logout</a>
  <%if (((String)request.getAttribute("level")).compareTo("admin") == 0) {%>
    <%if(request.getParameter("type").compareTo("update") == 0){%>
      <form method="get" action="/update">
        <a class="ui inverted blue button left" onclick="history.back();">Back</a>
        <input type="hidden" name = "id" value="<%=request.getParameter("id")%>">
        <input type="submit" name = "button" class="ui inverted blue button left" value="Update">
    <%}else{%>
      <a class="ui inverted blue button left" href="/search${back}">Back</a>
      <a class="ui inverted blue button right" href="/info?id=<%=request.getParameter("id")%>&type=update">Edit</a>
      <a class="ui inverted blue button right" href="/delete?id=<%=request.getParameter("id")%>">Delete</a>
  <%}}%>
  <p id="username" class="ui right">${username}</p>
</div>
<div id="details" class="center ui inverted segment">
  <%HashMap<String, Object> publication = (HashMap < String, Object >)request.getAttribute("information");%>
  <table class="ui selectable inverted table">
    <tbody>
    <tr class="centertext">
      <td>
        Publication
      </td>
    </tr>
    <tr>
      <td>
        Title: <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input placeholder="Title" name="title" type="text"
                 value = "<%=publication.get("title") == null ? "" : publication.get("title")%>">
        </div>
        <%} else {%> <%=publication.get("title") == null ? "Unknown" : publication.get("title")%> <%}%>
      </td>
    </tr>
    <tr>
      <td>
        Type: <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="type" type="text"
                 value = "<%=publication.get("type") == null ? "" : publication.get("type")%>">
        </div>
        <%} else {%> <%=publication.get("type") == null ? "Unknown" : publication.get("type")%> <%}%>
      </td>
    </tr>
    <tr>
      <td>
        Area: <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="area" type="text"
                 value = "<%=publication.get("area") == null ? "" : publication.get("area")%>">
        </div>
        <%} else {%> <%=publication.get("area") == null ? "Unknown" : publication.get("area")%> <%}%>
      </td>
    </tr>
    <tr>
      <td>
        Year: <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="year" type="text"
                 value = "<%=publication.get("year") == null ? "" : publication.get("year")%>">
        </div>
        <%} else {%> <%=publication.get("year") == null ? "Unknown" : publication.get("year")%> <%}%>
      </td>
    </tr>
    <tr>
      <td>
        <%if (request.getParameter("type").compareTo("update") == 0) {%>
        Source:
        <div class="ui input">
          <input name="url" type="text"
                 value = "<%=publication.get("url") == null ? "" : publication.get("url")%>">
        </div>
        <%} else {%> <a href="<%=publication.get("url") == null ? "Unknown" : publication.get("url")%>"> Link to source </a><%}%>
      </td>
    </tr>
    <tr class="centertext">
      <td>
        Authors
      </td>
    </tr>
    <tr>
      <td>
        <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="authors" type="text"
                 value = "<%=publication.get("authors") == null ? "" : publication.get("authors")%>">
        </div>
        <%} else {%> <%=publication.get("authors") == null ? "Unknown" : publication.get("authors")%> <%}%>
      </td>
    </tr>
    <tr class="centertext">
      <td>
        Institution
      </td>
    </tr>
    <tr>
      <td>
        Name: <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="insititution_name" type="text"
                 value = "<%=publication.get("institution_name") == null ? "" : publication.get("institution_name")%>">
        </div>
        <%} else {%> <%=publication.get("institution_name") == null ? "Unknown" : publication.get("institution_name")%> <%}%>
      </td>
    </tr>
    <tr>
      <td>
        Country: <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="insititution_country" type="text"
                 value = "<%=publication.get("institution__country") == null ? "" : publication.get("institution__country")%>">
        </div>
        <%} else {%> <%=publication.get("institution__country") == null ? "Unknown" : publication.get("institution__country")%> <%}%>
      </td>
    </tr>
    <tr>
      <td>
        City: <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="insititution_city" type="text"
                 value = "<%=publication.get("institution__city") == null ? "" : publication.get("institution__city")%>">
        </div>
        <%} else {%> <%=publication.get("institution__city") == null ? "Unknown" : publication.get("institution__city")%> <%}%>
      </td>
    </tr>
    <tr class="centertext">
      <td>
        Venue
      </td>
    </tr>
    <tr>
      <td>
        Name: <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="venue_name" type="text"
                 value = "<%=publication.get("venue_name") == null ? "" : publication.get("venue_name")%>">
        </div>
        <%} else {%> <%=publication.get("venue_name") == null ? "Unknown" : publication.get("venue_name")%> <%}%>
      </td>
    </tr>
    <tr>
      <td>
        Type: <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="venue_type" type="text"
                 value = "<%=publication.get("venue_type") == null ? "" : publication.get("venue_type")%>">
        </div>
        <%} else {%> <%=publication.get("venue_type") == null ? "Unknown" : publication.get("venue_type")%> <%}%>
      </td>
    </tr>
    <tr>
      <td>
        Doi: <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="doi" type="text"
                 value = "<%=publication.get("doi") == null ? "" : publication.get("doi")%>">
        </div>
        <%} else {%> <%=publication.get("doi") == null ? "Unknown" : publication.get("doi")%> <%}%>
      </td>
    </tr>
    <tr class="centertext">
      <td>
        Event
      </td>
    </tr>
    <tr>
      <td>
        Name: <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="event_name" type="text"
                 value = "<%=publication.get("event_name") == null ? "" : publication.get("event_name")%>">
        </div>
        <%} else {%> <%=publication.get("event_name") == null ? "Unknown" : publication.get("event_name")%> <%}%>
      </td>
    </tr>
    <tr>
      <td>
        Date: <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="event_date" type="text"
                 value = "<%=publication.get("event_date") == null ? "" : publication.get("event_date")%>">
        </div>
        <%} else {%> <%=publication.get("event_date") == null ? "Unknown" : publication.get("event_date")%> <%}%>
      </td>
    </tr>
    <tr class="centertext">
      <td>
        Keywords
      </td>
    </tr>
    <tr>
      <td>
        <%if (request.getParameter("type").compareTo("update") == 0) {%>
        <div class="ui input">
          <input name="keywords" type="text"
                 value = "<%=publication.get("keywords") == null ? "" : publication.get("keywords")%>">
        </div>
        <%} else {%> <%=publication.get("keywords") == null ? "Unknown" : publication.get("keywords")%> <%}%>
      </td>
    </tr>
    </tbody>
  </table>
  <%if (request.getParameter("type").compareTo("update") == 0) {%>
  </form>
  <%}%>
</div>
</body>
</html>

