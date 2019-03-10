#include "logger.h"
#include "prepared_statement_wrapper.h"
#include "worker.h"
#include "sqlexception.h"

#include <unistd.h>
#include <string.h>

PreparedStatementWrapper::PreparedStatementWrapper()
  : p_reconnectAttempts(0),
    p_stmt(0)
{}

PreparedStatementWrapper::~PreparedStatementWrapper() {
  close();
}

PreparedStatementWrapper* PreparedStatementWrapper::create(worker* w, const string& sql) {
  PreparedStatementWrapper* psw = new PreparedStatementWrapper();
  psw->p_worker = w;
  psw->p_query = sql;
  psw->reprepare();
  return psw;
}

void PreparedStatementWrapper::testConnection() {
  if (mysql_stat(p_worker->getConnection()) == 0) {
    LOG(logDetailed) << "Connection timed out, reconnecting...";
    LOG(logWarning) << "db reconnect not implemented yet";
  }
}

void PreparedStatementWrapper::close() {
  if (p_stmt) {
    mysql_stmt_close(p_stmt);
    p_stmt = 0;
  }
}

void PreparedStatementWrapper::reprepare() {
  if (p_stmt)
     mysql_stmt_reset(p_stmt);

  p_stmt = mysql_stmt_init(p_worker->getConnection());

  int ret = mysql_stmt_prepare(p_stmt, p_query.c_str(), p_query.length());
  if (ret)
    throw SQLException("mysql_stmt_prepare failed: "+string(mysql_stmt_error(p_stmt)));

  p_binds.clear();
  p_binds.resize(mysql_stmt_param_count(p_stmt));

  p_params.clear();
  p_params.resize(mysql_stmt_param_count(p_stmt));
}

// PROXIED FUNCTIONS

// bool PreparedStatementWrapper::execute(const string& sql) {
//   testConnection();
//   return p_stmt->execute(sql);
// }

bool PreparedStatementWrapper::execute() {
  testConnection();

  int ret = mysql_stmt_bind_param(p_stmt, p_binds.data());
  if (ret)
    throw SQLException("mysql_stmt_bind_param failed", p_stmt);

  ret = mysql_stmt_execute(p_stmt);
  if (ret)
    throw SQLException("mysql_stmt_execute failed", p_stmt);

  p_rowValid = false;
  return ret;
}

// sql::ResultSet* PreparedStatementWrapper::executeQuery(const string& sql) {
//   testConnection();
//   return p_stmt->executeQuery(sql);
// }

int PreparedStatementWrapper::executeQuery() {
  int ret = execute();

  ret = mysql_stmt_store_result(p_stmt);
  if (ret)
    throw SQLException("mysql_stmt_store_result failed", p_stmt);

  return rowsCount();
}

// int PreparedStatementWrapper::executeUpdate(const string& sql) {
//   testConnection();
//   return p_stmt->executeUpdate(sql);
// }

// int PreparedStatementWrapper::executeUpdate() {
//   testConnection();
//   return p_stmt->executeUpdate();
// }

// sql::ResultSetMetaData* PreparedStatementWrapper::getMetaData() {
//   testConnection();
//   return p_stmt->getMetaData();
// }

// sql::ParameterMetaData* PreparedStatementWrapper::getParameterMetaData() {
//   testConnection();
//   return p_stmt->getParameterMetaData();
// }

// bool PreparedStatementWrapper::getMoreResults() {
//   testConnection();
//   return p_stmt->getMoreResults();
// }

// void PreparedStatementWrapper::setBigInt(unsigned int parameterIndex, const string& value) {
//   testConnection();
//   return p_stmt->setBigInt(parameterIndex, value);
// }

// void PreparedStatementWrapper::setBlob(unsigned int parameterIndex, std::istream * blob) {
//   testConnection();
//   return p_stmt->setBlob(parameterIndex, blob);
// }

// void PreparedStatementWrapper::setBoolean(unsigned int parameterIndex, bool value) {
//   testConnection();
//   return p_stmt->setBoolean(parameterIndex, value);
// }

// void PreparedStatementWrapper::setDateTime(unsigned int parameterIndex, const string& value) {
//   testConnection();
//   return p_stmt->setDateTime(parameterIndex, value);
// }

// void PreparedStatementWrapper::setDouble(unsigned int parameterIndex, double value) {
//   testConnection();
//   return p_stmt->setDouble(parameterIndex, value);
// }

void PreparedStatementWrapper::setInt(unsigned int parameterIndex, int32_t value) {
  int32_t* tmp = new int32_t(value);
  p_binds[parameterIndex-1].buffer_type = MYSQL_TYPE_LONG;
  p_binds[parameterIndex-1].buffer = tmp;
  p_binds[parameterIndex-1].is_unsigned = false;
  p_params[parameterIndex-1].reset(tmp);
}

void PreparedStatementWrapper::setUInt(unsigned int parameterIndex, uint32_t value) {
  uint32_t* tmp = new uint32_t(value);
  p_binds[parameterIndex-1].buffer_type = MYSQL_TYPE_LONG;
  p_binds[parameterIndex-1].buffer = tmp;
  p_binds[parameterIndex-1].is_unsigned = true;
  p_params[parameterIndex-1].reset(tmp);
}

void PreparedStatementWrapper::setUInt64(unsigned int parameterIndex, uint64_t value) {
  uint64_t* tmp = new uint64_t(value);
  p_binds[parameterIndex-1].buffer_type = MYSQL_TYPE_LONGLONG;
  p_binds[parameterIndex-1].buffer = tmp;
  p_binds[parameterIndex-1].is_unsigned = true;
  p_params[parameterIndex-1].reset(tmp);
}

void PreparedStatementWrapper::setNull(unsigned int parameterIndex, int sqlType __attribute__((unused))) {
  p_binds[parameterIndex-1].buffer_type = MYSQL_TYPE_NULL;
  p_binds[parameterIndex-1].buffer = 0;
  p_params[parameterIndex-1].reset();
}

void PreparedStatementWrapper::setString(unsigned int parameterIndex, const string& value) {
  string* tmp = new string(value);
  p_binds[parameterIndex-1].buffer_type = MYSQL_TYPE_STRING;
  p_binds[parameterIndex-1].buffer = const_cast<char*>(tmp->c_str());
  p_binds[parameterIndex-1].buffer_length = tmp->length();
  p_params[parameterIndex-1].reset(tmp);
}

// sql::PreparedStatement* PreparedStatementWrapper::setResultSetType(sql::ResultSet::enum_type type) {
//   testConnection();
//   return p_stmt->setResultSetType(type);
// }

uint32_t PreparedStatementWrapper::getUInt(unsigned int index) {
  if (!p_rowValid)
    throw out_of_range("no result row available");
  if (index > mysql_stmt_field_count(p_stmt))
    throw out_of_range("field index out of range");

  uint32_t tmp;
  MYSQL_BIND bind = {};
  bind.buffer_type = MYSQL_TYPE_LONG;
  bind.buffer = &tmp;
  bind.buffer_length = sizeof(tmp);
  bind.is_unsigned = true;

  int ret = mysql_stmt_fetch_column(p_stmt, &bind, index-1, 0);
  if (ret)
    throw SQLException("failed to getUInt", p_stmt);

  return tmp;
}

uint64_t PreparedStatementWrapper::getUInt64(unsigned int index) {
  if (!p_rowValid)
    throw out_of_range("no result row available");
  if (index > mysql_stmt_field_count(p_stmt))
    throw out_of_range("field index out of range");

  uint64_t tmp;
  MYSQL_BIND bind = {};
  bind.buffer_type = MYSQL_TYPE_LONGLONG;
  bind.buffer = &tmp;
  bind.buffer_length = sizeof(tmp);
  bind.is_unsigned = true;

  int ret = mysql_stmt_fetch_column(p_stmt, &bind, index-1, 0);
  if (ret)
    throw SQLException("failed to getUInt64", p_stmt);

  return tmp;
}

std::string PreparedStatementWrapper::getString(unsigned int index) {
  if (!p_rowValid)
    throw out_of_range("no result row available");
  if (index > mysql_stmt_field_count(p_stmt))
    throw out_of_range("field index out of range");

  char tmp[256] = {};
  MYSQL_BIND bind = {};
  bind.buffer_type = MYSQL_TYPE_STRING;
  bind.buffer = tmp;
  bind.buffer_length = sizeof(tmp);

  int ret = mysql_stmt_fetch_column(p_stmt, &bind, index-1, 0);
  if (ret)
    throw SQLException("failed to getString", p_stmt);

  return string(tmp);
}

bool PreparedStatementWrapper::next() {
  p_rowValid = mysql_stmt_fetch(p_stmt) == 0;
  return p_rowValid;
}

void PreparedStatementWrapper::release() {
  mysql_stmt_free_result(p_stmt);
}

int PreparedStatementWrapper::rowsCount() {
  return mysql_stmt_num_rows(p_stmt);
}
