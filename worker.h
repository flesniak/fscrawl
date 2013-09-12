#ifndef WORKER_H
#define WORKER_H

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

  struct statistics {
    uint32_t files;
    uint32_t directories;
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
  void setTables(const string& directoryTable, const string& fileTable);

  const statistics& getStatistics() const;

  //The worker ascends to the specified path and returns its id, creates a directory if not specified different
  uint32_t ascendPath(string path, entry_t::type_t type = entry_t::directory, bool createDirectory = true);
  //Clear all database contents
  void clearDatabase();
  //Delete directory by id recursively
  void deleteDirectory(uint32_t id);
  //Delete file by id
  void deleteFile(uint32_t id);
  //The worker traces the path from id up to the base (including a faked path)
  string descendPath(uint32_t id, entry_t::type_t type);
  //Start to parse the directory path. path will be stripped from the files' path. If you want a path, use fakepath
  void parseDirectory(const string& path, uint32_t id = 0);
  //Verify the tree consistency. Resource hungry!
  void verifyTree();

private:
  void cacheDirectoryEntriesFromDB(uint32_t id, vector<entry_t*>& entryCache);
  void cacheParent(uint32_t id, uint32_t parent);
  entry_t getDirectoryById(uint32_t id);
  entry_t getDirectoryByName(const string& name, uint32_t parent);
  entry_t getFileById(uint32_t id);
  entry_t getFileByName(const string& name, uint32_t parent);
  void initDatabase();
  void inheritProperties(entry_t* parent, const entry_t* entry) const;
  uint32_t insertDirectory(uint32_t parent, const string& name, uint64_t size, time_t mtime);
  uint32_t insertFile(uint32_t parent, const string& name, uint64_t size, time_t mtime);
  void parseDirectory(const string& path, entry_t* ownEntry);
  void processChangedEntries(vector<entry_t*>& entries, entry_t* parentEntry);
  void updateDirectory(uint32_t parent, uint64_t size, time_t mtime);
  void updateFile(uint32_t parent, uint64_t size, time_t mtime);

  string errnoString();

  string p_basePath;
  bool p_databaseInitialized;
  string p_directoryTable;
  string p_fileTable;
  bool p_inheritMTime;
  bool p_inheritSize;
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