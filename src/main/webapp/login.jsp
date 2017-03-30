<%--
  Created by IntelliJ IDEA.
  User: Alexey
  Date: 15.10.2015
  Time: 20:37
  To change this template use File | Settings | File Templates.
--%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
         pageEncoding="windows-1251"%>
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Universe of knowledges</title>
  <link href="css/semantic.min.css" rel="stylesheet" type="text/css">
  <link href="css/styles.css" rel="stylesheet" type="text/css">
</head>
<body>
<div class="bar">
  <a class="ui inverted blue button left" href ="/change">${dbms}</a>
  <a class="ui inverted blue button" href="/login">Login</a>
  <a class="ui inverted blue button" href="/signup">Registration</a>
</div>
<form method="post" action="/login" class ="center auth">
    ${error}
  <div class="ui input">
    <input placeholder="Login" name="username" type="text">
  </div>
  <div class="ui input">
    <input placeholder="Password" name="password" type="password">
  </div>
  <input type="submit" class="ui blue button" name="commit" value="Login"></a>
</form>

</body>
</html>