<%--
  Created by IntelliJ IDEA.
  User: Alexey
  Date: 20.10.2015
  Time: 16:43
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
  <script language='javascript'>
    $(document).ready(function() {
      $('.ui.accordion').accordion();
    });
  </script>
  <script language='javascript'>
    $(document).ready(function() {
      $('.ui.dropdown').dropdown();
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

<form method="get" action="/search">
  <div id="big" class="ui center searches">
    <div class="ui form inverted segment">
      <div id="mainsearchbg" class="ui input">
        <input placeholder="Search by keywords" name="search" type="text">
      </div>
      <input type="submit" name = "button" class="ui blue button" value="Search">
      <div class="inline fields">
        <label class="">Sort by:</label>
        <div class="field">
          <div class="ui radio checkbox">
            <input name="sort" checked="checked" type="radio" value="title">
            <label>Title</label>
          </div>
        </div>
        <div class="field">
          <div class="ui radio checkbox">
            <input name="sort" type="radio" value = "year">
            <label>Year</label>
          </div>
        </div>
        <div class="field">
          <div class="ui radio checkbox">
            <input name="sort" type="radio" value = "type">
            <label>Type</label>
          </div>
        </div>
      </div>

      <div class="inline fields">
        <label>In order:</label>
        <div class="field">
          <div class="ui radio checkbox">
            <input name="order" checked="checked" type="radio" value="desc">
            <label>Descending</label>
          </div>
        </div>
        <div class="field">
          <div class="ui radio checkbox">
            <input name="order" type="radio" value="asc">
            <label>Ascending</label>
          </div>
        </div>
      </div>

      <div class="ui inverted accordion">
        <div class="title hidden">
          <i class="dropdown icon"></i> Publication
        </div>
        <div class="content hidden">
          <p style="display: block ! important;" class="transition visible">
          <div class="ui input">
            <input placeholder="Title" name="title" type="text">
          </div>
          <div class="ui input">
            <input placeholder="Area" name="area" type="text">
          </div>
          <div class="ui input">
            <input placeholder="Type" name="type" type="text">
          </div>
          <div class="ui input">
            <input placeholder="From year" name="from_year" type="text">
          </div>
          <div class="ui input">
            <input placeholder="To year" name="to_year" type="text">
          </div>
          </p>
        </div>
        <div class="title">
          <i class="dropdown icon"></i> Author
        </div>
        <div class="content">
          <p class="transition hidden">
          <div class="ui input">
            <input placeholder="Name" name="author_name" type="text">
          </div>
          </p>
        </div>
        <div class="title">
          <i class="dropdown icon"></i> Institution
        </div>
        <div class="content">
          <p class="transition hidden">
          <div class="ui input">
            <input placeholder="Name" name="institution_name" type="text">
          </div>
          <div class="ui input">
            <input placeholder="Country" name="institution_country" type="text">
          </div>
          <div class="ui input">
            <input placeholder="City" name="institution_city" type="text">
          </div>
          </p>
        </div>
        <div class="title">
          <i class="dropdown icon"></i> Venue
        </div>
        <div class="content">
          <p class="transition hidden">
          <div class="ui input">
            <input placeholder="Name" name="venue_name" type="text">
          </div>
          <div class="ui input">
            <input placeholder="Type" name="venue_type" type="text">
          </div>
          <div class="ui input">
            <input placeholder="DOI" name="doi" type="text">
          </div>
          </p>
        </div>
        <div class="title">
          <i class="dropdown icon"></i> Event
        </div>
        <div class="content">
          <p class="transition hidden">
          <div class="ui input">
            <input placeholder="Name" name="event_name" type="text">
          </div>
          <div class="ui input">
            <input placeholder="MM:DD:YY" name="event_date" type="text">
          </div>
          </p>
        </div>
      </div>
      <input type = "hidden" name = "page" value = 0>
    </div>
  </div>
</form>
</div>
</div>
</body>
</html>
