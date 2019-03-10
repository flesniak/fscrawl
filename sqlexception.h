#ifndef SQLEXCEPTION_H
#define SQLEXCEPTION_H

#include <exception>
#include <string>

#include <mysql.h>

class SQLException : public std::exception {
public:
  SQLException(const std::string& msg);
  SQLException(const std::string& msg, MYSQL* con);
  SQLException(const std::string& msg, MYSQL_STMT* stmt);
  const char* what() const throw();
private:
  SQLException(const std::string& msg, const std::string& detail);
  std::string p_str;
};

#endif //SQLEXCEPTION_H
