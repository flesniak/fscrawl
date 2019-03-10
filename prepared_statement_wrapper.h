#ifndef PREPARED_STATEMENT_WRAPPER_H
#define PREPARED_STATEMENT_WRAPPER_H

#include <memory>
#include <string>
#include <vector>

#include <mysql.h>

class worker;

class PreparedStatementWrapper {
public:
  ~PreparedStatementWrapper();
  static PreparedStatementWrapper* create(worker* w, const std::string& sql);

  void reprepare();
  void close();

  // bool execute(const std::string& sql);
  bool execute();

  // sql::ResultSet* executeQuery(const std::string& sql);
  int executeQuery();

  // int executeUpdate(const std::string& sql);
  // int executeUpdate();

  // sql::ResultSetMetaData * getMetaData();

  // sql::ParameterMetaData * getParameterMetaData();

  // bool getMoreResults();

  // void setBigInt(unsigned int parameterIndex, const std::string& value);

  // void setBlob(unsigned int parameterIndex, std::istream * blob);

  // void setBoolean(unsigned int parameterIndex, bool value);

  // void setDateTime(unsigned int parameterIndex, const std::string& value);

  // void setDouble(unsigned int parameterIndex, double value);

  void setInt(unsigned int parameterIndex, int32_t value);

  void setUInt(unsigned int parameterIndex, uint32_t value);

  void setInt64(unsigned int parameterIndex, int64_t value);

  void setUInt64(unsigned int parameterIndex, uint64_t value);

  void setNull(unsigned int parameterIndex, int sqlType);

  void setString(unsigned int parameterIndex, const std::string& value);

  // sql::PreparedStatement* setResultSetType(sql::ResultSet::enum_type type);

  // emulate sql::ResultSet
  uint32_t getUInt(unsigned int index);
  uint64_t getUInt64(unsigned int index);
  std::string getString(unsigned int index);
  bool next();
  void release(); // delete cached result data
  int rowsCount();

private:
  PreparedStatementWrapper();

  void testConnection();
  int p_reconnectAttempts;

  worker* p_worker;
  MYSQL_STMT* p_stmt;
  std::string p_query;
  std::vector<MYSQL_BIND> p_binds;
  std::vector<std::shared_ptr<void>> p_params;
  int p_resultOffset;
};

#endif //PREPARED_STATEMENT_WRAPPER_H
