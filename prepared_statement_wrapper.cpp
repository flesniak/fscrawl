#include "logger.h"
#include "prepared_statement_wrapper.h"
#include "worker.h"

#include <cppconn/connection.h>
#include <cppconn/prepared_statement.h>

#include <unistd.h>

PreparedStatementWrapper::PreparedStatementWrapper()
  : p_reconnectAttempts(0),
    p_stmt(0)
{}

PreparedStatementWrapper::~PreparedStatementWrapper() {
  if (p_stmt)
    delete p_stmt;
}

PreparedStatementWrapper* PreparedStatementWrapper::create(worker* w, const sql::SQLString& sql) {
  PreparedStatementWrapper* psw = new PreparedStatementWrapper();
  psw->p_worker = w;
  psw->p_string = sql;
  psw->reprepare();
  return psw;
}

void PreparedStatementWrapper::testConnection() {
  while (!p_worker->getConnection()->isValid()) {
    LOG(logDetailed) << "Connection timed out, reconnecting...";
    p_worker->getConnection()->reconnect();
    p_worker->databaseReconnected();
  }
}

void PreparedStatementWrapper::reprepare() {
  if (p_stmt)
    delete p_stmt;
  p_stmt = p_worker->getConnection()->prepareStatement(p_string);
}

// PROXIED FUNCTIONS

bool PreparedStatementWrapper::execute(const sql::SQLString& sql) {
  testConnection();
  return p_stmt->execute(sql);
}

bool PreparedStatementWrapper::execute() {
  testConnection();
  return p_stmt->execute();
}

sql::ResultSet* PreparedStatementWrapper::executeQuery(const sql::SQLString& sql) {
  testConnection();
  return p_stmt->executeQuery(sql);
}

sql::ResultSet* PreparedStatementWrapper::executeQuery() {
  testConnection();
  return p_stmt->executeQuery();
}

int PreparedStatementWrapper::executeUpdate(const sql::SQLString& sql) {
  testConnection();
  return p_stmt->executeUpdate(sql);
}

int PreparedStatementWrapper::executeUpdate() {
  testConnection();
  return p_stmt->executeUpdate();
}

sql::ResultSetMetaData* PreparedStatementWrapper::getMetaData() {
  testConnection();
  return p_stmt->getMetaData();
}

sql::ParameterMetaData* PreparedStatementWrapper::getParameterMetaData() {
  testConnection();
  return p_stmt->getParameterMetaData();
}

bool PreparedStatementWrapper::getMoreResults() {
  testConnection();
  return p_stmt->getMoreResults();
}

void PreparedStatementWrapper::setBigInt(unsigned int parameterIndex, const sql::SQLString& value) {
  testConnection();
  return p_stmt->setBigInt(parameterIndex, value);
}

void PreparedStatementWrapper::setBlob(unsigned int parameterIndex, std::istream * blob) {
  testConnection();
  return p_stmt->setBlob(parameterIndex, blob);
}

void PreparedStatementWrapper::setBoolean(unsigned int parameterIndex, bool value) {
  testConnection();
  return p_stmt->setBoolean(parameterIndex, value);
}

void PreparedStatementWrapper::setDateTime(unsigned int parameterIndex, const sql::SQLString& value) {
  testConnection();
  return p_stmt->setDateTime(parameterIndex, value);
}

void PreparedStatementWrapper::setDouble(unsigned int parameterIndex, double value) {
  testConnection();
  return p_stmt->setDouble(parameterIndex, value);
}

void PreparedStatementWrapper::setInt(unsigned int parameterIndex, int32_t value) {
  testConnection();
  return p_stmt->setInt(parameterIndex, value);
}

void PreparedStatementWrapper::setUInt(unsigned int parameterIndex, uint32_t value) {
  testConnection();
  return p_stmt->setUInt(parameterIndex, value);
}

void PreparedStatementWrapper::setInt64(unsigned int parameterIndex, int64_t value) {
  testConnection();
  return p_stmt->setInt64(parameterIndex, value);
}

void PreparedStatementWrapper::setUInt64(unsigned int parameterIndex, uint64_t value) {
  testConnection();
  return p_stmt->setUInt64(parameterIndex, value);
}

void PreparedStatementWrapper::setNull(unsigned int parameterIndex, int sqlType) {
  testConnection();
  return p_stmt->setNull(parameterIndex, sqlType);
}

void PreparedStatementWrapper::setString(unsigned int parameterIndex, const sql::SQLString& value) {
  testConnection();
  return p_stmt->setString(parameterIndex, value);
}

sql::PreparedStatement* PreparedStatementWrapper::setResultSetType(sql::ResultSet::enum_type type) {
  testConnection();
  return p_stmt->setResultSetType(type);
}

