#include "sqlexception.h"

#include <unistd.h>
#include <mysql.h>
#include <string>
#include <iostream>

using namespace std;

SQLException::SQLException(const string& msg) {
  p_str = msg;
}

SQLException::SQLException(const string& msg, MYSQL* con)
  : SQLException(msg, string(mysql_error(con))) {}

SQLException::SQLException(const string& msg, MYSQL_STMT* stmt)
  : SQLException(msg, string(mysql_stmt_error(stmt))) {}

const char* SQLException::what() const throw() {
  return p_str.c_str();
}

SQLException::SQLException(const string& msg, const string& detail)
  : SQLException(msg) {
  if (detail.length()) {
    p_str += ": ";
    p_str += detail;
  }
}
