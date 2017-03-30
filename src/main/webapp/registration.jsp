<%--
  Created by IntelliJ IDEA.
  User: Alexey
  Date: 16.10.2015
  Time: 16:09
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Universe of knowledges</title>

  <link href="css/semantic.min.css" rel="stylesheet" type="text/css">
  <link href="css/styles.css" rel="stylesheet" type="text/css">
</head>
<body>
<div class="bar">
  <a class="ui inverted blue button" href="/login">Login</a>
  <a class="ui inverted blue button" href="/signup">Registration</a>
</div>
<form method="post" action="/signup" class ="center auth">
  ${error}
  <div class="ui input">
    <input placeholder="Login" name="username" type="text">
  </div>
  <div class="ui input">
    <input placeholder="Password" name="password" type="password">
  </div>
  <div class="ui input">
    <input placeholder="Confirm password" name="confirm" type="password">
  </div>
    <div class="ui input">
      <input placeholder="Superpassword (leave it if you are not admin)" name="superpass" type="password">
    </div>
  <input type="submit" class="ui blue button" name="commit" value="Registration"></a>
</form>
</body>
</html>
