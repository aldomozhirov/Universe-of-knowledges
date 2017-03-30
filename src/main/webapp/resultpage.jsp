<%@ page import="java.util.List" %>
<%@ page import="java.util.HashMap" %>
<%--
  Created by IntelliJ IDEA.
  User: Alexey
  Date: 30.10.2015
  Time: 11:07
  To change this template use File | Settings | File Templates.
--%>

<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Universe of knowledges</title>
  <link href="css/semantic.min.css" rel="stylesheet" type="text/css">
  <link href="css/styles.css" rel="stylesheet" type="text/css">
  <script src="js/jquery-2.1.4.min.js"></script>
  <script src="js/semantic.min.js"></script>
  <script language='javascript'>
    $(document).ready(function() {
      $('.ui.accordion').accordion();
    });
  </script>
</head>
<body>
<div class="bar">
  <a class="ui inverted blue button right" href="/logout">Logout</a>
<%if (((String)request.getAttribute("level")).compareTo("admin") == 0) {%>
  <a class="ui inverted blue button right" href="/insert">Insert</a>
  <%}%>
  <p id="username" class="ui right">${username}</p>
</div>
<div id="schandres" class="center">

  <form class="left" method="get" action="/search">
    <div id="small" class="ui searches">
      <div class="ui form inverted segment">
        <div id="mainsearchsm" class="ui input">
          <input placeholder="Search by keywords" name="search" type="text" value= "<%=request.getParameter("search")%>">
        </div>
        <input name = "button" type="submit" class="ui blue button" value="Search">
        <div class="inline fields">
          <label>Sort by:</label>
          <div class="field">
            <div class="ui radio checkbox">
              <%String old_sort = request.getParameter("sort");%>
              <input name="sort" <%=old_sort.compareTo("title") == 0 ? "checked" : ""%> type="radio" value="title">
              <label>Title</label>
            </div>
          </div>
          <div class="field">
            <div class="ui radio checkbox">
              <input name="sort" <%=old_sort.compareTo("year") == 0 ? "checked" : ""%> type="radio" value = "year">
              <label>Year</label>
            </div>
          </div>
          <div class="field">
            <div class="ui radio checkbox">
              <input name="sort" <%=old_sort.compareTo("type") == 0 ? "checked" : ""%> type="radio" value = "type">
              <label>Type</label>
            </div>
          </div>
        </div>
        <div class="inline fields">
          <label>In order:</label>
          <div class="field">
            <div class="ui radio checkbox">
              <%String old_order = request.getParameter("order");%>
              <input name="order" <%=old_order.compareTo("desc") == 0 ? "checked" : ""%> type="radio" value="desc">
              <label>Descending</label>
            </div>
          </div>
          <div class="field">
            <div class="ui radio checkbox">
              <input name="order" <%=old_order.compareTo("asc") == 0 ? "checked" : ""%> type="radio" value="asc">
              <label>Ascending</label>
            </div>
          </div>
        </div>
        <div class="ui inverted accordion">
          <div class="title ${active0}">
            <i class="dropdown icon"></i> Publication
          </div>
          <div class="content ${active0}">
            <p style="display: block ! important;" class="transition visible">
            <div class="ui input">
              <input placeholder="Title" name="title" type="text" value="<%=request.getParameter("title")%>">
            </div>
            <div class="ui input">
              <input placeholder="Area" name="area" type="text" value="<%=request.getParameter("area")%>">
            </div>
            <div class="ui input">
              <input placeholder="Type" name="type" type="text" value="<%=request.getParameter("type")%>">
            </div>
            <div class="ui input">
              <input placeholder="From year" name="from_year" type="text" value="<%=request.getParameter("from_year")%>">
            </div>
            <div class="ui input">
              <input placeholder="To year" name="to_year" type="text" value="<%=request.getParameter("to_year")%>">
            </div>
            </p>
          </div>
          <div class="title ${active1}">
            <i class="dropdown icon"></i> Author
          </div>
          <div class="content ${active1}">
            <p class="transition hidden">
            <div class="ui input">
              <input placeholder="Name" name="author_name" type="text" value="<%=request.getParameter("author_name")%>">
            </div>
            </p>
          </div>
          <div class="title ${active2}">
            <i class="dropdown icon"></i> Institution
          </div>
          <div class="content ${active2}">
            <p class="transition hidden">
            <div class="ui input">
              <input placeholder="Name" name="institution_name" type="text" value="<%=request.getParameter("institution_name")%>">
            </div>
            <div class="ui input">
              <input placeholder="Country" name="institution_country" type="text" value="<%=request.getParameter("institution_country")%>">
            </div>
            <div class="ui input">
              <input placeholder="City" name="institution_city" type="text" value="<%=request.getParameter("institution_city")%>">
            </div>
            </p>
          </div>
          <div class="title ${active3}">
            <i class="dropdown icon"></i> Venue
          </div>
          <div class="content ${active3}">
            <p class="transition hidden">
            <div class="ui input">
              <input placeholder="Name" name="venue_name" type="text" value="<%=request.getParameter("venue_name")%>">
            </div>
            <div class="ui input">
              <input placeholder="Type" name="venue_type" type="text" value="<%=request.getParameter("venue_type")%>">
            </div>
            <div class="ui input">
              <input placeholder="DOI" name="doi" type="text" value="<%=request.getParameter("doi")%>">
            </div>
            </p>
          </div>
          <div class="title ${active4}">
            <i class="dropdown icon"></i> Event
          </div>
          <div class="content ${active4}">
            <p class="transition hidden">
            <div class="ui input">
              <input placeholder="Name" name="event_name" type="text" value="<%=request.getParameter("event_name")%>">
            </div>
            <div class="ui input">
              <input placeholder="MM:DD:YY" name="event_date" type="text" value="<%=request.getParameter("event_date")%>">
            </div>
            </p>
          </div>
        </div>
        <input name = "button" type="submit" class="ui blue button" value="<<">
        <input name = "button" type="submit" class="ui blue button" value="<">
        ${page + 1}/${pages + 1}
        <input name = "button" type="submit" class="ui blue button" value=">">
        <input name = "button" type="submit" class="ui blue button" value=">>">
    </div>
    </div>
  </form>


  <table id="results" class="ui selectable inverted table">
    <tbody>
      <% List<HashMap<String, Object>> data = (List<HashMap<String, Object>>) request.getAttribute("data");
        for (HashMap<String, Object> tuple : data )
        {
      %>
      <tr>
        <td>
          <a href="/info?id=<%=tuple.get("id")%>&type=show">
            <div style="height:100%;width:100%;color:#fff">
          <%=tuple.containsKey("title") ? tuple.get("title") : ""%>
          <ul>
            <li><%=tuple.get("authors")%></li>
            <li><%=tuple.get("year")%></li>
            <li><%=tuple.get("type")%></li>
          </ul></div>
          </a>
        </td>
      </tr>
      <%}%>
    </tbody>
  </table>
</div>
</body>
</html>