<%@ page import="java.util.HashMap" %>
<%--
  Created by IntelliJ IDEA.
  User: Alexey
  Date: 17.11.2015
  Time: 23:25
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
  <a class="ui inverted blue button left" onclick="history.back();">Back</a>
  <p id="username" class="ui right">${username}</p>
  <form method="get" action="/insert">
    <input type="submit" name = "button" class="ui inverted blue button left" value="Insert">
</div>
<div id="details" class="center ui inverted segment">
  <table class="ui selectable inverted table">
    <tbody>
    <tr class="centertext">
      <td>
        Publication
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "Title*" name="title" type="text">
        </div>
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "Type*" name="type" type="text">
        </div>
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "Area*" name="area" type="text">
        </div>
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "Year*" name="year" type="text">
        </div>
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "URL*" name="url" type="text">
        </div>
      </td>
    </tr>
    <tr class="centertext">
      <td>
        Authors
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "Names of authors" name="authors" type="text">
        </div>
      </td>
    </tr>
    <tr class="centertext">
      <td>
        Institution
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "Name" name="insititution_name" type="text">
        </div>
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "Country" name="insititution_country" type="text">
        </div>
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "City" name="insititution_city" type="text">
        </div>
      </td>
    </tr>
    <tr class="centertext">
      <td>
        Venue
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "Name" name="venue_name" type="text">
        </div>
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "Type" name="venue_type" type="text">
        </div>
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "DOI" name="doi" type="text">
        </div>
      </td>
    </tr>
    <tr class="centertext">
      <td>
        Event
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "Name" name="event_name" type="text">
        </div>
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "Date" name="event_date" type="text">
        </div>
      </td>
    </tr>
    <tr class="centertext">
      <td>
        Keywords
      </td>
    </tr>
    <tr>
      <td>
        <div class="ui input">
          <input placeholder = "Related words" name="keywords" type="text">
        </div>
      </td>
    </tr>
    </tbody>
  </table>
  </form>
</div>
</body>
</html>

