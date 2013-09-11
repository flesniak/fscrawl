#include "worker.h"
#include "logger.h"

#include <cerrno>
#include <cstring>
#include <sstream>
#include <iostream>

#include <dirent.h>
#include <sys/stat.h>

worker::worker(sql::Connection* dbConnection) : p_databaseInitialized(false),
                                                p_directoryTable("fscrawl_directories"),
                                                p_fileTable("fscrawl_files"),
                                                p_inheritMTime(false),
                                                p_inheritSize(true),
                                                p_connection(dbConnection),
                                                p_prepQueryFileById(0),
                                                p_prepQueryFileByName(0),
                                                p_prepQueryFilesByParent(0),
                                                p_prepInsertFile(0),
                                                p_prepUpdateFile(0),
                                                p_prepDeleteFile(0),
                                                p_prepDeleteFiles(0),
                                                p_prepQueryDirById(0),
                                                p_prepQueryDirByName(0),
                                                p_prepQueryDirsByParent(0),
                                                p_prepInsertDir(0),
                                                p_prepUpdateDir(0),
                                                p_prepDeleteDir(0),
                                                p_prepLastInsertID(0),
                                                p_stmt(p_connection->createStatement()) {
}

worker::~worker() {
}

uint32_t worker::ascendPath(string path, entry_t::type_t type, bool createDirectory) {
  uint32_t pathId = 0;
  if( !p_databaseInitialized )
    initDatabase();
  if( !path.empty() ) {
    LOG(logDetailed) << "Ascending into specified path " << path;
    while( !path.empty() ) {
      string pathElement = path.substr(0,path.find('/'));
      LOG(logDebug) << "Getting path element " << pathElement;
      path.erase(0,pathElement.length()+1); //erase pathElement and slash
      if( pathElement.empty() ) //strinheritedProperties leading and multinheritedPropertiesle slashes
        continue;
      entry_t subEntry = getDirectoryByName(pathElement,pathId);
      if( subEntry.id == 0 ) { //directory not found
        if( type == entry_t::directory ) {
          if( createDirectory )
            subEntry.id = insertDirectory(pathId,pathElement,0,time(0));
          else
            throw("unable to ascend path, unexistent directory");
        } else {
          subEntry = getFileByName(pathElement,pathId);
        }
      }
      pathId = subEntry.id;
    }
  }
  return pathId;
}

void worker::cacheDirectoryEntriesFromDB(uint32_t id, vector<entry_t*>& entryCache) {
  entryCache.clear();

  p_prepQueryDirsByParent->setUInt(1,id);
  sql::ResultSet* res = p_prepQueryDirsByParent->executeQuery();
  while( res->next() ) {
    entry_t* entry = new entry_t;
    entry->type = entry_t::directory;
    entry->state = entry_t::entryUnknown;
    entry->id = res->getUInt(1);
    entry->parent = id;
    entry->name = res->getString(2);
    entry->size = res->getUInt64(3);
    entry->mtime = res->getUInt(4);
    entryCache.push_back(entry);
    LOG(logDebug) << "cache: got dir id " << entry->id << " parent " << entry->parent << " name " << entry->name << " size " << entry->size << " mtime " << entry->mtime;

  }
  delete res;

  p_prepQueryFilesByParent->setUInt(1,id);
  res = p_prepQueryFilesByParent->executeQuery();
  while( res->next() ) {
    entry_t* entry = new entry_t;
    entry->type = entry_t::file;
    entry->state = entry_t::entryUnknown;
    entry->id = res->getUInt(1);
    entry->parent = id;
    entry->name = res->getString(2);
    entry->size = res->getUInt64(3);
    entry->mtime = res->getUInt(4);
    entryCache.push_back(entry);
    LOG(logDebug) << "cache: got file id " << entry->id << " parent " << entry->parent << " name " << entry->name << " size " << entry->size << " mtime " << entry->mtime;
  }
  delete res;
}

void worker::clearDatabase() {
  LOG(logWarning) << "Clearing database, dropping tables";
  p_stmt->execute("DROP TABLE "+p_fileTable);
  p_stmt->execute("DROP TABLE "+p_directoryTable);
  LOG(logWarning) << "ALL DATA IS NOW GONE";
  p_databaseInitialized = false;
}

void worker::deleteDirectory(uint32_t id) { //completely delete directory "id" including all subdirs/files
  p_prepQueryDirsByParent->setUInt(1,id);
  sql::ResultSet *res = p_prepQueryDirsByParent->executeQuery();
  while( res->next() )
    deleteDirectory( res->getUInt(1) ); //first, delete every subdirectory
  delete res;
  p_prepDeleteFiles->setUInt(1,id);
  p_prepDeleteFiles->execute(); //now, delete every file in this directory
  p_prepDeleteDir->setUInt(1,id);
  p_prepDeleteDir->execute(); //finally, delete this directory
}

void worker::deleteFile(uint32_t id) {
  p_prepDeleteFile->setUInt(1,id);
  p_prepDeleteFile->execute();
}

string worker::descendPath(uint32_t id, entry_t::type_t type) {
  if( id != 0 ) {
    entry_t e;
    if( type == entry_t::file )
      e = getFileById(id);
    else
      e = getDirectoryById(id);
    if( e.name.empty() )
      throw("unable to descend path, unexistent entry");
    else
      return descendPath(e.parent,entry_t::directory)+'/'+e.name;
  } else
    return "/";
}

string worker::errnoString() {
   char* e = strerror(errno);
   return e ? e : "";
}

worker::entry_t worker::getDirectoryById(uint32_t id) {
  entry_t e = { id, 0, string(), 0, 0, entry_t::entryUnknown, entry_t::directory };
  p_prepQueryDirById->setUInt(1,id);
  sql::ResultSet* res = p_prepQueryDirById->executeQuery();
  if( res->next() ) {
    e.name = res->getString(1);
    e.parent = res->getUInt(2);
    e.size = res->getUInt64(3);
    e.mtime = res->getUInt(4);
  }
  delete res;
  return e;
}

worker::entry_t worker::getDirectoryByName(const string& name, uint32_t parent) {
  entry_t e = { 0, 0, name, parent, 0, entry_t::entryUnknown, entry_t::directory };
  p_prepQueryDirByName->setUInt(1,parent);
  p_prepQueryDirByName->setString(2,name);
  sql::ResultSet* res = p_prepQueryDirByName->executeQuery();
  if( res->next() ) {
    e.id = res->getUInt(1);
    e.size = res->getUInt64(2);
    e.mtime = res->getUInt(3);
  }
  delete res;
  return e;
}

worker::entry_t worker::getFileById(uint32_t id) {
  entry_t e = { id, 0, string(), 0, 0, entry_t::entryUnknown, entry_t::file };
  p_prepQueryFileById->setUInt(1,id);
  sql::ResultSet* res = p_prepQueryFileById->executeQuery();
  if( res->next() ) {
    e.name = res->getString(1);
    e.parent = res->getUInt(2);
    e.size = res->getUInt64(3);
    e.mtime = res->getUInt(4);
  }
  delete res;
  return e;
}

worker::entry_t worker::getFileByName(const string& name, uint32_t parent) {
  entry_t e = { 0, 0, name, parent, 0, entry_t::entryUnknown, entry_t::file };
  p_prepQueryFileByName->setUInt(1,parent);
  p_prepQueryFileByName->setString(2,name);
  sql::ResultSet* res = p_prepQueryFileByName->executeQuery();
  if( res->next() ) {
    e.id = res->getUInt(1);
    e.size = res->getUInt64(2);
    e.mtime = res->getUInt(3);
  }
  delete res;
  return e;
}

const worker::statistics& worker::getStatistics() const {
  return p_statistics;
}

void worker::initDatabase() {
  LOG(logDebug) << "create tables if not exists"; //create database tables in case they do not exist
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

  LOG(logDebug) << "preparing statements";
  if( p_prepQueryFileById != 0 ) delete p_prepQueryFileById;
  if( p_prepQueryFileByName != 0 ) delete p_prepQueryFileByName;
  if( p_prepQueryFilesByParent != 0 ) delete p_prepQueryFilesByParent;
  if( p_prepInsertFile != 0 ) delete p_prepInsertFile;
  if( p_prepUpdateFile != 0 ) delete p_prepUpdateFile;
  if( p_prepDeleteFile != 0 ) delete p_prepDeleteFile;
  if( p_prepDeleteFiles != 0 ) delete p_prepDeleteFiles;
  if( p_prepQueryDirById != 0 ) delete p_prepQueryDirById;
  if( p_prepQueryDirByName != 0 ) delete p_prepQueryDirByName;
  if( p_prepQueryDirsByParent != 0 ) delete p_prepQueryDirsByParent;
  if( p_prepInsertDir != 0 ) delete p_prepInsertDir;
  if( p_prepUpdateDir != 0 ) delete p_prepUpdateDir;
  if( p_prepDeleteDir != 0 ) delete p_prepDeleteDir;
  if( p_prepLastInsertID != 0 ) delete p_prepLastInsertID;

  //p_prepare heavily used mysql functions
  p_prepQueryFileById = p_connection->prepareStatement("SELECT name,parent,size,UNIX_TIMESTAMP(date) FROM "+p_fileTable+" WHERE id=?");
  p_prepQueryFileByName = p_connection->prepareStatement("SELECT id,size,UNIX_TIMESTAMP(date) FROM "+p_fileTable+" WHERE parent=? AND name=?");
  p_prepQueryFilesByParent = p_connection->prepareStatement("SELECT id,name,size,UNIX_TIMESTAMP(date) FROM "+p_fileTable+" WHERE parent=?");
  p_prepInsertFile = p_connection->prepareStatement("INSERT INTO "+p_fileTable+" (name,parent,size,date) VALUES (?, ?, ?, FROM_UNIXTIME(?))");
  p_prepUpdateFile = p_connection->prepareStatement("UPDATE "+p_fileTable+" SET size=?, date=FROM_UNIXTIME(?) WHERE id=?");
  p_prepDeleteFile = p_connection->prepareStatement("DELETE FROM "+p_fileTable+" WHERE id=?");
  p_prepDeleteFiles = p_connection->prepareStatement("DELETE FROM "+p_fileTable+" WHERE parent=?");

  p_prepQueryDirById = p_connection->prepareStatement("SELECT name,parent,size,UNIX_TIMESTAMP(date) FROM "+p_directoryTable+" WHERE id=?");
  p_prepQueryDirByName = p_connection->prepareStatement("SELECT id,size,UNIX_TIMESTAMP(date) FROM "+p_directoryTable+" WHERE parent=? AND name=?");
  p_prepQueryDirsByParent = p_connection->prepareStatement("SELECT id,name,size,UNIX_TIMESTAMP(date) FROM "+p_directoryTable+" WHERE parent=?");
  p_prepInsertDir = p_connection->prepareStatement("INSERT INTO "+p_directoryTable+" (name,parent,size,date) VALUES (?, ?, ?, FROM_UNIXTIME(?))");
  p_prepUpdateDir = p_connection->prepareStatement("UPDATE "+p_directoryTable+" SET size=?, date=FROM_UNIXTIME(?) WHERE id=?");
  p_prepDeleteDir = p_connection->prepareStatement("DELETE FROM "+p_directoryTable+" WHERE id=?");

  p_prepLastInsertID = p_connection->prepareStatement("SELECT LAST_INSERT_ID()");

  p_statistics.files = 0;
  p_statistics.directories = 0;
}

uint32_t worker::insertDirectory(uint32_t parent, const string& name, uint64_t size, time_t mtime) {
  LOG(logDetailed) << "Adding directory " << name;
  LOG(logDebug) << "inserting dir parent " << parent << " name " << name << " size " << size << " mtime " << mtime;

  p_prepInsertDir->setString(1,name);
  p_prepInsertDir->setUInt(2,parent);
  p_prepInsertDir->setUInt(3,size);
  p_prepInsertDir->setUInt(4,mtime);
  p_prepInsertDir->execute();

  uint32_t dirId = 0;
  sql::ResultSet* res = p_prepLastInsertID->executeQuery();
  if( res->next() )
    dirId = res->getUInt(1);
  else
    LOG(logError) << "ERROR: Insert statement failed for " << name;
  delete res;
  return dirId;
}

uint32_t worker::insertFile(uint32_t parent, const string& name, uint64_t size, time_t mtime) {
  LOG(logDetailed) << "Adding file " << name;
  LOG(logDebug) << "inserting file parent " << parent << " name " << name << " size " << size << " mtime " << mtime;

  p_prepInsertFile->setString(1,name);
  p_prepInsertFile->setUInt(2,parent);
  p_prepInsertFile->setUInt(3,size);
  p_prepInsertFile->setUInt(4,mtime);
  p_prepInsertFile->execute();

  uint32_t fileId = 0;
  sql::ResultSet* res = p_prepLastInsertID->executeQuery();
  if( res->next() )
    fileId = res->getUInt(1);
  else
    LOG(logError) << "ERROR: Insert statement failed for " << name;
  delete res;
  return fileId;
}

worker::inheritedProperties_t worker::parseDirectory(const string& path, uint32_t id) {
  if( !p_databaseInitialized )
    initDatabase();

  DIR* dir; //directory pointer
  struct dirent* dirEntry; //directory entry
  struct stat64 dirEntryStat; //directory's stat
  inheritedProperties_t inheritedProperties = {0, 0};
  vector<entry_t*> entryCache;

  LOG(logDetailed) << "Processing directory " << path;

  dir = opendir(path.c_str());
  if( dir == NULL ) {
    cout << "ERROR: failed to read directory " << path << ": " << errnoString() << endl;
    return inheritedProperties;
  }

  LOG(logDebug) << "fetching directory entries from db for caching";
  cacheDirectoryEntriesFromDB(id, entryCache);

  while( ( dirEntry = readdir(dir) ) ) { //readdir returns NULL when "end" of directory is reached
    if( strcmp(dirEntry->d_name,".") == 0 || strcmp(dirEntry->d_name,"..") == 0 ) //don't process . and .. for obvious reasons
      continue;
    string dirEntryPath = path + "/" + dirEntry->d_name;
    LOG(logDebug) << "processing dirEntry " << dirEntryPath;
    if( stat64(dirEntryPath.c_str(), &dirEntryStat) ) {
      cout << "ERROR: stat() on " << dirEntryPath << " failed: " << errnoString() << endl;
      continue;
    }

    entry_t* entry = 0;
    for( vector<entry_t*>::iterator it = entryCache.begin(); it != entryCache.end(); it++ ) //try to get entry from cache
      if( (*it)->name == dirEntry->d_name ) {
        entry = *it;
        break;
      }

    //treat type changed file<->directory with same name
    if( entry != 0 &&
        ( ( entry->type == entry_t::file && S_ISDIR(dirEntryStat.st_mode) ) ||
          ( entry->type == entry_t::directory && !S_ISDIR(dirEntryStat.st_mode) ) ) ) {
      entry->state = entry_t::entryDeleted; //mark entry to be deleted
      entry = 0; //act as if that entry was not found
    }

    if( entry == 0 ) { //entry does not exist in db yet
      entry = new entry_t;
      entry->state = entry_t::entryNew;
      if( S_ISDIR(dirEntryStat.st_mode) )
        entry->type = entry_t::directory;
      else
        entry->type = entry_t::file;
      entry->id = 0;
      entry->parent = id;
      entry->name = dirEntry->d_name;
      entry->mtime = dirEntryStat.st_mtime;
      entry->size = dirEntryStat.st_size;
      entryCache.push_back(entry);
    } else { //entry is in db, check for changes
      if( entry->size != (uint64_t)dirEntryStat.st_size && ( entry->type == entry_t::file || !p_inheritSize ) ) {
        entry->size = dirEntryStat.st_size;
        entry->state = entry_t::entryPropertiesChanged;
      }
      if( entry->mtime != dirEntryStat.st_mtime && ( entry->type == entry_t::file || !p_inheritSize ) ) {
        entry->mtime = dirEntryStat.st_mtime;
        entry->state = entry_t::entryPropertiesChanged;
      }
      if( entry->state == entry_t::entryUnknown )
        entry->state = entry_t::entryOk;
    }

    if( entry->type == entry_t::file )
      p_statistics.files++;
    else
      p_statistics.directories++;
  }

  //adds size, updates mtime if larger
  inheritedProperties += processChangedEntries(entryCache, entry_t::file);
  for( vector<entry_t*>::iterator it = entryCache.begin(); it != entryCache.end(); it++ )
    if( (*it)->type == entry_t::directory ) {
      inheritedProperties_t inheritedProperties = parseDirectory(path + "/" + (*it)->name, (*it)->id);
      if( p_inheritSize ) {
        (*it)->size = inheritedProperties.size;
        inheritedProperties.size += inheritedProperties.size;
      }
      if( p_inheritMTime ) {
        if( (*it)->mtime > inheritedProperties.mtime )
          (*it)->mtime = inheritedProperties.mtime;
        if( inheritedProperties.mtime > inheritedProperties.mtime )
          inheritedProperties.mtime += inheritedProperties.mtime;
      }
    }
  processChangedEntries(entryCache, entry_t::directory);

  LOG(logDebug) << "leaving directory " << path;
  closedir(dir);
  return inheritedProperties;
}

worker::inheritedProperties_t worker::processChangedEntries(vector<entry_t*>& entries, entry_t::type_t type) {
  inheritedProperties_t inheritedProperties = {0, 0};
  vector<entry_t*>::iterator it = entries.begin();
  while( it != entries.end() ) {
    if( type != entry_t::any && (*it)->type != type ) {
      it++;
      continue;
    }

    if( (*it)->type == entry_t::directory )
      switch( (*it)->state ) {
        case entry_t::entryOk : {
          break;
        }
        case entry_t::entryPropertiesChanged : {
          updateDirectory( (*it)->id, (*it)->size, (*it)->mtime );
          break;
        }
        case entry_t::entryNew : {
          insertDirectory( (*it)->id, (*it)->name, (*it)->size, (*it)->mtime );
          break;
        }
        case entry_t::entryUnknown : //continue to entryDeleted
        case entry_t::entryDeleted : {
          deleteDirectory( (*it)->id );
          break;
        }
        default : {
          LOG(logError) << "bug: unhandled entry_t::state";
          break;
        }
      }

    if( (*it)->type == entry_t::file ) {
      switch( (*it)->state ) {
        case entry_t::entryOk : {
          inheritedProperties.inherit( (*it)->size, (*it)->mtime );
          break;
        }
        case entry_t::entryPropertiesChanged : {
          updateFile( (*it)->id, (*it)->size, (*it)->mtime );
          inheritedProperties.inherit( (*it)->size, (*it)->mtime );
          break;
        }
        case entry_t::entryNew : {
          insertFile( (*it)->id, (*it)->name, (*it)->size, (*it)->mtime );
          inheritedProperties.inherit( (*it)->size, (*it)->mtime );
          break;
        }
        case entry_t::entryUnknown : //continue to entryDeleted
        case entry_t::entryDeleted : {
          deleteFile( (*it)->id );
          break;
        }
        default : {
          LOG(logError) << "bug: unhandled entry_t::state";
          break;
        }
      }
    }

    entries.erase(it);
  }
  return inheritedProperties;
}

void worker::setConnection(sql::Connection* dbConnection) {
  p_connection = dbConnection;
  p_databaseInitialized = false;
}

void worker::setInheritance(bool inheritSize, bool inheritMTime) {
  p_inheritSize = inheritSize;
  p_inheritMTime = inheritMTime;
}

void worker::setTables(const string& directoryTable, const string& fileTable) {
  if( !directoryTable.empty() )
    p_directoryTable = directoryTable;
  if( !fileTable.empty() )
    p_fileTable = fileTable;
  p_databaseInitialized = false;
}

void worker::updateDirectory(uint32_t id, uint64_t size, time_t mtime) {
  LOG(logDetailed) << "Updating directory id " << id;
  LOG(logDebug) << "updating dir id " << id << " size " << size << " mtime " << mtime;
  p_prepUpdateDir->setUInt64(1,size);
  p_prepUpdateDir->setUInt(2,mtime);
  p_prepUpdateDir->setUInt(3,id);
  p_prepUpdateDir->execute();
}

void worker::updateFile(uint32_t id, uint64_t size, time_t mtime) {
  LOG(logDetailed) << "Updating file id " << id;
  LOG(logDebug) << "updating file id " << id << " size " << size << " mtime " << mtime;
  p_prepUpdateDir->setUInt64(1,size);
  p_prepUpdateDir->setUInt(2,mtime);
  p_prepUpdateDir->setUInt(3,id);
  p_prepUpdateDir->execute();
}

//verifies the complete tree, deletes orphaned entries
//resource intensive, traces every entry up to the root or a cached (valid) parent id
void worker::verifyTree() {
  if( !p_databaseInitialized )
    initDatabase();
  map<uint32_t,uint32_t> parentIdCache; // <id, parent> key are directory ids, value is the directories parent id
  map<uint32_t,uint32_t>::iterator cacheIterator;
  sql::PreparedStatement *p_prepGetDirParent = p_connection->prepareStatement("SELECT parent FROM "+p_directoryTable+" WHERE id=?");

  LOG(logDetailed) << "Verifying directories";
  sql::ResultSet *res = p_stmt->executeQuery("SELECT id,parent FROM "+p_directoryTable);
  while( res->next() ) { //loop through all directories
    uint32_t id = res->getUInt(1);
    uint32_t parent = res->getUInt(2);
    if( parent == 0 ) //parent 0 is always valid
      continue;
    //now we check if we can find the given parent of id
    if( (cacheIterator = parentIdCache.find(parent)) != parentIdCache.end() ) { //we already know this parent, it's valid!
      LOG(logDebug) << "found parent " << parent << " of id " << id << " in cache, valid";
      parentIdCache[id] = parent;
    } else { //we dont know that parent yet, ask the database
      p_prepGetDirParent->setUInt(1,parent);
      sql::ResultSet *parentQueryResult = p_prepGetDirParent->executeQuery();
      if( parentQueryResult->next() ) { //parent exists, cache it - saves us query time on the files later on
        LOG(logDebug) << "found parent " << parent << " of id " << id << " in database, caching it, valid";
        parentIdCache[id] = parent;
        parentIdCache[parent] = parentQueryResult->getUInt(1);
      } else {
        LOG(logWarning) << "Parent " << parent << " of directory " << id << " does not exist. Deleting that subtree!";
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
      LOG(logWarning) << "Parent " << parent << " of directory " << id << " does not exist. Deleting that subtree!";
      deleteDirectory(id);
    }
    delete parentQueryResult;
  }
  delete res;

  LOG(logDetailed) << "Verifying files";
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
