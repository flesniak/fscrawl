#include "worker.h"

#include <cerrno>
#include <cstring>
#include <sstream>
#include <iostream>

#include <dirent.h>
#include <sys/stat.h>


worker::worker(sql::Connection* dbConnection) : p_databaseInitialized(false), p_baseId(0), p_connection(dbConnection), p_directoryTable("fscrawl_directories"), p_fileTable("fscrawl_files") {
  sql::Statement* p_stmt = p_connection->createStatement();

  //p_prepare heavily used mysql functions
  p_prepQueryFile = p_connection->prepareStatement("SELECT id,size,date FROM "+p_fileTable+" WHERE parent=? AND name=?");
  p_prepInsertFile = p_connection->prepareStatement("INSERT INTO "+p_fileTable+" (name,parent,size,date) VALUES (?, ?, ?, FROM_UNIXTIME(?))");
  p_prepUpdateFileSize = p_connection->prepareStatement("UPDATE "+p_fileTable+" SET size=?, date=FROM_UNIXTIME(?) WHERE id=?");
  p_prepUpdateFileDate = p_connection->prepareStatement("UPDATE "+p_fileTable+" SET date=FROM_UNIXTIME(?) WHERE id=?");

  p_prepQueryDir = p_connection->prepareStatement("SELECT id,date FROM "+p_directoryTable+" WHERE parent=? AND name=?");
  p_prepInsertDir = p_connection->prepareStatement("INSERT INTO "+p_directoryTable+" (name,parent,date) VALUES (?, ?, FROM_UNIXTIME(?))");
  p_prepUpdateDirSize = p_connection->prepareStatement("UPDATE "+p_directoryTable+" SET size=? WHERE id=?"); //directories won't have their date changed on size change
  p_prepUpdateDirDate = p_connection->prepareStatement("UPDATE "+p_directoryTable+" SET date=FROM_UNIXTIME(?) WHERE id=?");

  p_prepLastInsertID = p_connection->prepareStatement("SELECT LAST_INSERT_ID()");
}

worker::~worker() {
}

//inserts directory "name" under "parent" if not exists and (in any case) returns its id
uint32_t worker::addDirectory(uint32_t parent, const string& name, time_t mtime) {
  uint32_t dirId;
  p_prepQueryDir->setUInt(1,parent);
  p_prepQueryDir->setString(2,name);
  sql::ResultSet *res = p_prepQueryDir->executeQuery();
  if( res->next() ) {
    log("directory "+name+" is in db, id "+res->getString(1),debug);
    dirId = res->getInt(1);
    if( res->isNull(2) ) {
      p_prepUpdateDirDate->setUInt(1,mtime);
      p_prepUpdateDirDate->setUInt(2,dirId);
      p_prepUpdateDirDate->execute();
    }
  } else {
    log("Directory "+name+" is new, inserting it into the database",detailed);
    p_prepInsertDir->setString(1,name);
    p_prepInsertDir->setUInt(2,parent);
    p_prepInsertDir->setUInt(3,mtime);
    p_prepInsertDir->execute();
    delete res;
    res = p_prepLastInsertID->executeQuery();
    if( res->next() )
      dirId = res->getInt(1);
    else {
      cout << "ERROR: Insert statement failed for " << name << endl;
      exit(5);
    }
  }
  p_statistics.directories++;
  delete res;
  return dirId;
}

//inserts file "name" under "parent" with "size" if not exists and (in any case) returns its id
uint32_t worker::addFile(uint32_t parent, const string& name, uint64_t size, time_t mtime) {
  p_prepQueryFile->setUInt(1,parent);
  p_prepQueryFile->setString(2,name);
  uint32_t fileId;
  sql::ResultSet *res = p_prepQueryFile->executeQuery();
  if( res->next() ) {
    log("file "+name+" is in db, id "+res->getString(1),debug);
    fileId = res->getInt(1);
    if( size != res->getUInt64(2) ) {
      log("The file "+name+" altered its size, updating",status);
      p_prepUpdateFileSize->setUInt64(1,size);
      p_prepUpdateFileSize->setUInt(2,mtime);
      p_prepUpdateFileSize->setUInt(3,fileId);
      p_prepUpdateFileSize->execute();
    }
    //insert 
    if( res->isNull(3) ) {
      p_prepUpdateFileDate->setUInt(1,mtime);
      p_prepUpdateFileDate->setUInt(2,fileId);
      p_prepUpdateFileDate->execute();
    }
  } else {
    log("File "+name+" is new, inserting it into the database",detailed);
    p_prepInsertFile->setString(1,name);
    p_prepInsertFile->setUInt(2,parent);
    p_prepInsertFile->setUInt64(3,size);
    p_prepInsertFile->setUInt(4,mtime);
    p_prepInsertFile->execute();
    delete res;
    res = p_prepLastInsertID->executeQuery();
    if( res->next() )
      fileId = res->getInt(1);
    else {
      cout << "ERROR: Insert statement failed for " << name << endl;
      exit(5);
    }
  }
  delete res;
  p_statistics.files++;
  return fileId;
}

void worker::ascendFakepath(string fakepath) {
  if( !p_databaseInitialized )
    initDatabase();
  if( !fakepath.empty() ) {
    log("Ascending into specified fakepath \""+fakepath+"\"",detailed);
    while( !fakepath.empty() ) {
      string temppath = fakepath.substr(0,fakepath.find('/'));
      log("getting fakepath element \""+temppath+"\"",debug);
      fakepath.erase(0,temppath.length()+1);
      if( temppath.empty() ) //strip leading and multiple slashes
        continue;
      p_baseId = addDirectory(p_baseId,temppath,time(0));
    }
  }
}

void worker::clearDatabase() {
  log("Clearing database, dropping tables",status);
  p_stmt->execute("DROP TABLE "+p_fileTable);
  p_stmt->execute("DROP TABLE "+p_directoryTable);
  log("ALL DATA IS NOW GONE",status);
  p_databaseInitialized = false;
}

void worker::deleteDirectory(uint32_t id) { //completely delete directory "id" including all subdirs/files
  stringstream ss;
  ss << "SELECT id FROM "+p_directoryTable+" WHERE parent=" << id;
  sql::ResultSet *res = p_stmt->executeQuery(ss.str());
  while( res->next() )
    deleteDirectory( res->getInt(1) ); //first, delete every subdirectory
  ss.str("");
  ss << "DELETE FROM "+p_fileTable+" WHERE parent=" << id;
  p_stmt->execute(ss.str()); //now, delete every file in this directory
  ss.str("");
  ss << "DELETE FROM "+p_directoryTable+" WHERE id=" << id;
  p_stmt->execute(ss.str()); //finally, delete this directory
}

void worker::deleteOrphanedFiles(uint32_t id) {
  stringstream ss;

  log("deleting orphaned files and directories in id "+id,debug);
  ss << "DELETE FROM "+p_fileTable+" WHERE parent=" << id;
  if( !p_presentFileCache.empty() ) {
    ss << " AND id NOT IN (";
    for(vector<uint32_t>::iterator it = p_presentFileCache.begin(); it != p_presentFileCache.end(); it++) {
      if( it != p_presentFileCache.begin() )
         ss << ",";
      ss << *it;
    }
    ss << ")";
  }
  p_stmt->execute(ss.str());

  ss.str("");
  ss << "SELECT id FROM "+p_directoryTable+" WHERE parent=" << id;
  if( !p_presentDirectoryCache.empty() ) {
    ss << " AND id NOT IN (";
    for(vector<uint32_t>::iterator it = p_presentDirectoryCache.begin(); it != p_presentDirectoryCache.end(); it++) {
      if( it != p_presentDirectoryCache.begin() )
         ss << ",";
      ss << *it;
    }
    ss << ")";
  }
  sql::ResultSet *res = p_stmt->executeQuery(ss.str());
  while( res->next() )
    deleteDirectory( res->getInt(1) );
}

string worker::errnoString() {
   char* e = strerror(errno);
   return e ? e : "";
}

const worker::statistics& worker::getStatistics() const {
  return p_statistics;
}

void worker::initDatabase() {
  log("create tables if not exists",debug); //create database tables in case they do not exist
  p_stmt->execute("CREATE TABLE IF NOT EXISTS "+p_directoryTable+
                  "(id INT UNSIGNED NOT NULL AUTO_INCREMENT KEY,"
                  "name VARCHAR(255) NOT NULL,"
                  "parent INT UNSIGNED NOT NULL,"
                  "size BIGINT UNSIGNED,"
                  "date DATETIME DEFAULT NULL,"
                  "INDEX(parent))"
                  "DEFAULT CHARACTER SET utf8 "
                  "COLLATE utf8_bin"); //utf8_bin collation against errors with umlauts, e.g. two directories named "Moo" and "Möo"
  p_stmt->execute("CREATE TABLE IF NOT EXISTS "+p_fileTable+
                  "(id INT UNSIGNED NOT NULL AUTO_INCREMENT KEY,"
                  "name VARCHAR(255) NOT NULL,"
                  "parent INT UNSIGNED NOT NULL,"
                  "size BIGINT UNSIGNED,"
                  "date DATETIME DEFAULT NULL,"
                  "INDEX(parent))"
                  "DEFAULT CHARACTER SET utf8 "
                  "COLLATE utf8_bin"); //utf8_bin collation against errors with umlauts, e.g. two files named "Moo" and "Möo"
  p_statistics.files = 0;
  p_statistics.directories = 0;
}

inline void worker::log(const string& message, logLevel level) {
  if( level >= p_logLevel )
    cout << "V" << (uint32_t)level << ": " << message << endl;
}

uint64_t worker::parseDirectory(const string& path, uint32_t id) {
  if( !p_databaseInitialized )
    initDatabase();

  DIR *dirpointer; //directory pointer
  struct dirent *direntry; //directory entry
  struct stat64 dirstat; //directory's stat
  uint32_t subid;
  time_t mtime;
  uint64_t dirsize = 0;

  log("Processing directory "+path,detailed);

  dirpointer = opendir(path.c_str());
  if( dirpointer == NULL ) {
    cout << "ERROR: failed to read directory " << path << ": " << errnoString() << endl;
    return 0;
  }

  while( ( direntry = readdir(dirpointer) ) ) { //readdir returns NULL when "end" of directory is reached
    if( strcmp(direntry->d_name,".") == 0 || strcmp(direntry->d_name,"..") == 0 ) //don't process . and .. for obvious reasons
      continue;
    string subpath = path + "/" + direntry->d_name;
    if( stat64(subpath.c_str(), &dirstat) ) {
      cout << "ERROR: stat() on " << subpath << " failed: " << errnoString() << endl;
      continue;
    }
    mtime = dirstat.st_mtime;
    if( S_ISDIR(dirstat.st_mode) ) {
      subid = addDirectory(id,direntry->d_name,mtime); //add directory usind id as its parent
      dirsize += parseDirectory(subpath,subid); //parse that subdirectory!
      p_presentFileCache.push_back(subid); //remember its id for cleanup
    } else {
      subid = addFile(id,direntry->d_name,dirstat.st_size,mtime); //add file using id as its parent and supplying its size
      p_presentFileCache.push_back(subid); //remember its id for cleanup
      dirsize += dirstat.st_size;
    }
  }
  deleteOrphanedFiles(id);
  p_prepUpdateDirSize->setUInt64(1,dirsize);
  p_prepUpdateDirSize->setUInt(2,id);
  p_prepUpdateDirSize->execute();
  p_presentFileCache.clear(),
  p_presentFileCache.clear();
  log("leaving directory "+path,debug);
  closedir(dirpointer);
  return dirsize;
}

void worker::setConnection(sql::Connection* dbConnection) {
  p_connection = dbConnection;
  p_databaseInitialized = false;
}

void worker::setLogLevel(logLevel level) {
  p_logLevel = level;
}

void worker::setTables(const string& directoryTable, const string& fileTable) {
  p_directoryTable = directoryTable;
  p_fileTable = fileTable;
  p_databaseInitialized = false;
}

//verifies the complete tree, deletes orphaned entries
//resource intensive, traces every entry up to the root or a cached (valid) parent id
void worker::verifyTree() {
  if( !p_databaseInitialized )
    initDatabase();
  map<uint32_t,uint32_t> parentIdCache; // <id, parent> key are directory ids, value is the directories parent id
  map<uint32_t,uint32_t>::iterator cacheIterator;
  sql::PreparedStatement *p_prepGetDirParent = p_connection->prepareStatement("SELECT parent FROM "+p_directoryTable+" WHERE id=?");

  log("Verifying directories",detailed);
  sql::ResultSet *res = p_stmt->executeQuery("SELECT id,parent FROM "+p_directoryTable);
  while( res->next() ) { //loop through all directories
    uint32_t id = res->getUInt(1);
    uint32_t parent = res->getUInt(2);
    if( parent == 0 ) //parent 0 is always valid
      continue;
    //now we check if we can find the given parent of id
    if( (cacheIterator = parentIdCache.find(parent)) != parentIdCache.end() ) { //we already know this parent, it's valid!
      stringstream ss;
      ss << "found parent " << parent << " of id " << id << " in cache, valid";
      log(ss.str(),debug);
      parentIdCache[id] = parent;
    } else { //we dont know that parent yet, ask the database
      p_prepGetDirParent->setUInt(1,parent);
      sql::ResultSet *parentQueryResult = p_prepGetDirParent->executeQuery();
      if( parentQueryResult->next() ) { //parent exists, cache it - saves us query time on the files later on
        stringstream ss;
        ss << "found parent " << parent << " of id " << id << " in database, caching it, valid";
        log(ss.str(),debug);
        parentIdCache[id] = parent;
        parentIdCache[parent] = parentQueryResult->getUInt(1);
      } else {
        stringstream ss;
        ss << "Parent " << parent << " of directory " << id << " does not exist. Deleting that subtree!";
        log(ss.str(),status);
        deleteDirectory(id);
        //now we have to remove possibly incorrectly cached ids
        uint32_t tempParentId = id;
        // TODO
      }
      delete parentQueryResult;
    }
    
    
    
    
    
    p_prepGetDirParent->setUInt(1,parent);
    sql::ResultSet *parentQueryResult = p_prepGetDirParent->executeQuery();
    if( parentQueryResult->next() ) { //parent exists, cache it - saves us query time on the files later on
      parentIdCache[id] = parent;
      parentIdCache[parent] = parentQueryResult->getUInt(1);
    }
    else {
      stringstream ss;
      ss << "Parent " << parent << " of directory " << id << " does not exist. Deleting that subtree!";
      log(ss.str(),status);
      deleteDirectory(id);
    }
    delete parentQueryResult;
  }
  delete res;

  log("Verifying files",debug);
  res = p_stmt->executeQuery("SELECT id,parent FROM "+p_fileTable);
  while( res->next() ) {
    uint32_t id = res->getUInt(1);
    uint32_t parent = res->getUInt(2);
    if( parent == 0 || parentIdCache.count(parent) == 0 ) {
      stringstream ss;
      ss << "Parent " << parent << " of file " << id << " does not exist. Deleting that file";
      ss.str("");
      ss << "DELETE FROM "+p_fileTable+" WHERE id=" << id;
      p_stmt->executeQuery(ss.str());
    }
  }
  delete res;
}
