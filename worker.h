#ifndef WORKER_H
#define WORKER_H

#include <map>
#include <string>
#include <vector>

#include <cppconn/driver.h>
#include <cppconn/prepared_statement.h>
#include <mysql_connection.h>

using namespace std;

class worker {
public:
  worker(sql::Connection* dbConnection = 0);
  ~worker();

  enum logLevel { quiet, status, detailed, debug };
  struct statistics {
    uint32_t files;
    uint32_t directories;
  };

  void setConnection(sql::Connection* dbConnection);
  void setLogLevel(logLevel level);
  void setTables(const string& directoryTable, const string& fileTable);

  const statistics& getStatistics() const;

  //The worker ascends to the specified fakepath: e.g. fakepath=="files", this creates the virtual directory /files, and all crawled files in BasePath will have /files as prefix
  void ascendFakepath(string fakepath);
  //Clear all database contents
  void clearDatabase();
  //Start to parse the directory path. path will be stripped from the files' path. If you want a path, use fakepath
  uint64_t parseDirectory(const string& path, uint32_t id = 0);
  //Verify the tree consistency. Resource hungry!
  void verifyTree();

private:
  void cacheParent(uint32_t id, uint32_t parent);
  void getParent(uint32_t id);
  void initDatabase();
  uint32_t addDirectory(uint32_t parent, const string& name, time_t mtime);
  uint32_t addFile(uint32_t parent, const string& name, uint64_t size, time_t mtime);
  void deleteDirectory(uint32_t id);
  void deleteOrphanedFiles(uint32_t id);

  string errnoString();
  void log(const string& message, logLevel level);

  bool p_databaseInitialized;
  logLevel p_logLevel;
  uint32_t p_baseId;
  string p_basePath;
  string p_directoryTable;
  string p_fileTable;
  map<uint32_t,uint32_t> p_parentIdCache;
  vector<uint32_t> p_presentDirectoryCache;
  vector<uint32_t> p_presentFileCache;
  statistics p_statistics;

  sql::Connection* p_connection;
  sql::PreparedStatement* p_prepQueryFile;
  sql::PreparedStatement* p_prepInsertFile;
  sql::PreparedStatement* p_prepQueryDir;
  sql::PreparedStatement* p_prepInsertDir;
  sql::PreparedStatement* p_prepLastInsertID;
  sql::PreparedStatement* p_prepUpdateDirSize;
  sql::PreparedStatement* p_prepUpdateFileSize;
  sql::PreparedStatement* p_prepUpdateDirDate;
  sql::PreparedStatement* p_prepUpdateFileDate;
  sql::Statement* p_stmt;
};

#endif //WORKER_H