#ifndef PREPARED_STATEMENT_WRAPPER_H
#define PREPARED_STATEMENT_WRAPPER_H

#include <cppconn/resultset.h>
#include <cppconn/sqlstring.h>

namespace sql {
  class Connection;
  class PreparedStatement;
  class ParameterMetaData;
  class ResultSetMetaData;
  class ResultSet;
};

class worker;

class PreparedStatementWrapper {
public:
  ~PreparedStatementWrapper();
  static PreparedStatementWrapper* create(worker* w, const sql::SQLString& sql);

  //void clearParameters();
  void reprepare();

  bool execute(const sql::SQLString& sql);
  bool execute();

  sql::ResultSet* executeQuery(const sql::SQLString& sql);
  sql::ResultSet* executeQuery();

  int executeUpdate(const sql::SQLString& sql);
  int executeUpdate();

  sql::ResultSetMetaData * getMetaData();

  sql::ParameterMetaData * getParameterMetaData();

  bool getMoreResults();

  void setBigInt(unsigned int parameterIndex, const sql::SQLString& value);

  void setBlob(unsigned int parameterIndex, std::istream * blob);

  void setBoolean(unsigned int parameterIndex, bool value);

  void setDateTime(unsigned int parameterIndex, const sql::SQLString& value);

  void setDouble(unsigned int parameterIndex, double value);

  void setInt(unsigned int parameterIndex, int32_t value);

  void setUInt(unsigned int parameterIndex, uint32_t value);

  void setInt64(unsigned int parameterIndex, int64_t value);

  void setUInt64(unsigned int parameterIndex, uint64_t value);

  void setNull(unsigned int parameterIndex, int sqlType);

  void setString(unsigned int parameterIndex, const sql::SQLString& value);

  sql::PreparedStatement* setResultSetType(sql::ResultSet::enum_type type);

private:
  PreparedStatementWrapper();

  void testConnection();
  int p_reconnectAttempts;

  worker* p_worker;
  sql::PreparedStatement* p_stmt;
  sql::SQLString p_string;
};

#endif //PREPARED_STATEMENT_WRAPPER_H