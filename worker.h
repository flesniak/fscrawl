#ifndef WORKER_H
#define WORKER_H

#include <map>
#include <string>
#include <utility>
#include <vector>

#include <mysql.h>
#include <stdint.h>

#include "prepared_statement_wrapper.h"

using namespace std;

class Hasher;

class worker {
public:
  worker(MYSQL* dbConnection = 0);
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
    uint64_t subSize; //used to calculate the size of directories
    enum state_t { entryUnknown, entryOk, entryDeleted, entryPropertiesChanged, entryNew } state;
    /* entryUnknown: fresh entries from database (cached or directly retrieved), not yet checked against filesystem
     * entryOk: entry stats equal filesystem stats
     * entryDeleted: entry was deleted in filesystem
     * entryPropertiesChanged: entry stats changed, have to be updated in db
     * entryNew: entry not in database yet
     */
    enum type_t { file, directory, any } type;
    string hash; //only valid for files
  };

  void setConnection(MYSQL* dbConnection);
  MYSQL* getConnection() const;
  void setInheritance(bool inheritSize, bool inheritMTime);
  void setDryRun(bool on);
  void setTables(const string& directoryTable, const string& fileTable);
  void setHasher(Hasher* hasher);
  Hasher* getHasher() const;
  void setForceHashing(bool force);
  bool getForceHashing() const;

  const statistics& getStatistics() const;
  void resetStatistics();

  //The worker traces the path from "id" up the id "upToId", its name is not appended to the returned path anymore.
  string ascendPath(uint32_t id, uint32_t upToId = 0, entry_t::type_t type = entry_t::file);
  //The worker descends to the specified path and returns its id, creates a directory if not specified different
  uint32_t descendPath(string path, entry_t::type_t type = entry_t::directory, bool createDirectory = true);
  //Clear all database contents
  void clearDatabase();
  //Delete directory by id recursively
  void deleteDirectory(uint32_t id);
  //Delete file by id
  void deleteFile(uint32_t id);
  //Start to parse the directory path. path will be stripped from the files' path. Will be inserted under parent "id".
  void parseDirectory(const string& path, uint32_t id = 0);
  //Verify the tree consistency. Resource hungry!
  void verifyTree();
  //Prints all files and directories in the tree below parent
  void printTree(uint32_t parent = 0, const string& path = "");
  //Watches the directory id including subdirectories
  void watch(const string& path, uint32_t id = 0);
  //Check the hash of all files under directory "parent", prepending "path" to the files relative path from the database
  //Only files existing in the database will be crawled, the filesystem path is built from database information.
  void hashCheck(const string& path, uint32_t parent = 0);
  //Called by PreparedStatementWrapper if the connection has been reconnected and thus all prepared statements have to be re-prepared
  void databaseReconnected();
  //Sets internal conditions to abort crawling, hashing or watching
  void abort();

  //returns strerror as std::string
  static string errnoString();

private:
  void cacheDirectoryEntriesFromDB(uint32_t id, vector<entry_t*>& entryCache);
  void cacheParent(uint32_t id, uint32_t parent);
  //These functions access the database and get their stored properties.
  entry_t getDirectoryById(uint32_t id); //returns an empty entry_t.name on failure
  entry_t getDirectoryByName(const string& name, uint32_t parent); //returns entry_t.id = 0 on failure
  entry_t getFileById(uint32_t id); //returns an empty entry_t.name on failure
  entry_t getFileByName(const string& name, uint32_t parent); //returns entry_t.id = 0 on failure
  void initDatabase();
  void prepareStatements();
  void inheritProperties(entry_t* parent, const entry_t* entry) const;
  uint32_t insertDirectory(uint32_t parent, const string& name, uint64_t size, time_t mtime);
  uint32_t insertFile(uint32_t parent, const string& name, uint64_t size, time_t mtime, const string& hash);
  //parses everything inside path, uses the id specified in ownEntry. size and mtime of contents will be updates into ownEntry as well. does not change the directory itself in the db
  void parseDirectory(const string& path, entry_t* ownEntry);
  void processChangedEntries(vector<entry_t*>& entries, entry_t* parentEntry);
  //tries to read a file or directory at the specified path and returns its properties (name, size, mtime) in an entry_t
  entry_t readPath(const string& path); //returns entry_t.state = entry_t::entryOk/entryUnknown on success/failure
  void removeWatches(uint32_t id);
  void setupWatches(const string& path, uint32_t id);
  void updateDirectory(uint32_t id, uint64_t size, time_t mtime);
  void updateFile(uint32_t id, uint64_t size, time_t mtime, const string& hash);
  void updateTreeProperties(uint32_t firstParent, int64_t sizeDiff, time_t newMTime);
  void hashFile(entry_t* entry, const string& path) const;

  void query(const string& query);

  string p_basePath;
  bool p_databaseInitialized;
  string p_directoryTable;
  string p_fileTable;
  bool p_inheritMTime;
  bool p_inheritSize;
  statistics p_statistics;
  int p_watchDescriptor;
  map< int, pair<uint32_t,string> > p_watches; //stores inotify watch descriptors and their corresponding ids and paths
  bool p_forceHashing;
  bool p_run;
  bool p_dryRun;

  Hasher* p_hasher;

  MYSQL* p_connection;
  PreparedStatementWrapper* p_prepQueryFileById;
  PreparedStatementWrapper* p_prepQueryFileByName;
  PreparedStatementWrapper* p_prepQueryFilesByParent;
  PreparedStatementWrapper* p_prepInsertFile;
  PreparedStatementWrapper* p_prepUpdateFile;
  PreparedStatementWrapper* p_prepDeleteFile;
  PreparedStatementWrapper* p_prepDeleteFiles;
  PreparedStatementWrapper* p_prepQueryDirById;
  PreparedStatementWrapper* p_prepQueryDirByName;
  PreparedStatementWrapper* p_prepQueryDirsByParent;
  PreparedStatementWrapper* p_prepQueryParentOfDir;
  PreparedStatementWrapper* p_prepInsertDir;
  PreparedStatementWrapper* p_prepUpdateDir;
  PreparedStatementWrapper* p_prepDeleteDir;
  PreparedStatementWrapper* p_prepLastInsertID;
};

#endif //WORKER_H
