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
  struct inheritedProperties_t {
    uint64_t size;
    time_t mtime;
    void inherit(uint64_t size, time_t mtime) {
      this->size += size;
      if( mtime > this->mtime )
        this->mtime = mtime;
    };
    void operator+=(const inheritedProperties_t& t) {
      size += t.size;
      if( t.mtime > mtime )
        mtime = t.mtime;
    };
  };
  struct entry_t {
    uint32_t id;
    time_t mtime;
    string name;
    uint32_t parent;
    uint64_t size;
    enum state_t { entryUnknown, entryOk, entryDeleted, entryPropertiesChanged, entryNew } state;
    enum type_t { file, directory, any } type;
  };

  void setConnection(sql::Connection* dbConnection);
  void setInheritance(bool inheritSize, bool inheritMTime);
  void setLogLevel(logLevel level);
  void setTables(const string& directoryTable, const string& fileTable);

  const statistics& getStatistics() const;

  //The worker ascends to the specified path and returns its id, creates a directory if not specified different
  uint32_t ascendPath(string path, entry_t::type_t type = entry_t::directory, bool createDirectory = true);
  //Clear all database contents
  void clearDatabase();
  //The worker traces the path from id up to the base (including a faked path)
  string descendPath(uint32_t id, entry_t::type_t type);
  //Start to parse the directory path. path will be stripped from the files' path. If you want a path, use fakepath
  inheritedProperties_t parseDirectory(const string& path, uint32_t id = 0);
  //Verify the tree consistency. Resource hungry!
  void verifyTree();

private:
  void cacheDirectoryEntriesFromDB(uint32_t id, vector<entry_t*>& entryCache);
  void cacheParent(uint32_t id, uint32_t parent);
  void deleteDirectory(uint32_t id);
  void deleteFile(uint32_t id);
  entry_t getDirectoryById(uint32_t id);
  entry_t getDirectoryByName(const string& name, uint32_t parent);
  entry_t getFileById(uint32_t id);
  entry_t getFileByName(const string& name, uint32_t parent);
  void initDatabase();
  uint32_t insertDirectory(uint32_t parent, const string& name, uint64_t size, time_t mtime);
  uint32_t insertFile(uint32_t parent, const string& name, uint64_t size, time_t mtime);
  inheritedProperties_t processChangedEntries(vector<entry_t*>& entries, entry_t::type_t type = entry_t::any);
  void updateDirectory(uint32_t parent, uint64_t size, time_t mtime);
  void updateFile(uint32_t parent, uint64_t size, time_t mtime);

  string errnoString();
  void log(const string& message, logLevel level);

  string p_basePath;
  bool p_databaseInitialized;
  string p_directoryTable;
  string p_fileTable;
  bool p_inheritMTime;
  bool p_inheritSize;
  uint32_t p_lastLogLength;
  logLevel p_logLevel;
  statistics p_statistics;

  sql::Connection* p_connection;
  sql::PreparedStatement* p_prepQueryFileById;
  sql::PreparedStatement* p_prepQueryFileByName;
  sql::PreparedStatement* p_prepQueryFilesByParent;
  sql::PreparedStatement* p_prepInsertFile;
  sql::PreparedStatement* p_prepUpdateFile;
  sql::PreparedStatement* p_prepDeleteFile;
  sql::PreparedStatement* p_prepDeleteFiles;
  sql::PreparedStatement* p_prepQueryDirById;
  sql::PreparedStatement* p_prepQueryDirByName;
  sql::PreparedStatement* p_prepQueryDirsByParent;
  sql::PreparedStatement* p_prepInsertDir;
  sql::PreparedStatement* p_prepUpdateDir;
  sql::PreparedStatement* p_prepDeleteDir;
  sql::PreparedStatement* p_prepLastInsertID;
  sql::Statement* p_stmt;
};

#endif //WORKER_H