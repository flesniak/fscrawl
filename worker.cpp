#include "worker.h"
#include "logger.h"
#include "hasher.h"
#include "options.h"
#include "sqlexception.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <list>
#include <sstream>

#include <dirent.h>
#include <unistd.h>
#include <sys/inotify.h>
#include <sys/stat.h>
#include <poll.h>

worker::worker(MYSQL* dbConnection) : p_databaseInitialized(false),
                                      p_directoryTable("fscrawl_directories"),
                                      p_fileTable("fscrawl_files"),
                                      p_inheritMTime(false),
                                      p_inheritSize(true),
                                      p_watchDescriptor(0),
                                      p_forceHashing(0),
                                      p_run(true),
                                      p_dryRun(false),
                                      p_hasher(0),
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
                                      p_prepQueryParentOfDir(0),
                                      p_prepInsertDir(0),
                                      p_prepUpdateDir(0),
                                      p_prepDeleteDir(0),
                                      p_prepLastInsertID(0) {
}

worker::~worker() {
}

void worker::abort() {
  p_run = false;
}

void worker::setDryRun(bool on) {
  LOG(logInfo) << "performing dry run";
  p_dryRun = on;
}

string worker::ascendPath(uint32_t id, uint32_t downToId, entry_t::type_t type) {
  if( !p_databaseInitialized )
    initDatabase();
  if( id != downToId ) {
    entry_t e;
    if( type == entry_t::file )
      e = getFileById(id);
    else
      e = getDirectoryById(id);
    if( e.name.empty() )
      throw("unable to ascend path, inexistent entry");
    else
      return ascendPath(e.parent,downToId,entry_t::directory)+'/'+e.name;
  } else
    return ""; //don't attach a leading slash
}

void worker::cacheDirectoryEntriesFromDB(uint32_t id, vector<entry_t*>& entryCache) {
  entryCache.clear();

  p_prepQueryDirsByParent->setUInt(1,id);
  p_prepQueryDirsByParent->executeQuery();
  while( p_prepQueryDirsByParent->next() ) {
    entry_t* entry = new entry_t;
    entry->type = entry_t::directory;
    entry->state = entry_t::entryUnknown;
    entry->id = p_prepQueryDirsByParent->getUInt(1);
    entry->parent = id;
    entry->name = p_prepQueryDirsByParent->getString(2);
    entry->size = p_prepQueryDirsByParent->getUInt64(3);
    entry->mtime = p_prepQueryDirsByParent->getUInt(4);
    entryCache.push_back(entry);
    LOG(logDebug) << "cache: got dir id " << entry->id << " parent " << entry->parent << " name " << entry->name << " size " << entry->size << " mtime " << entry->mtime;
  }
  p_prepQueryDirsByParent->release();

  p_prepQueryFilesByParent->setUInt(1,id);
  p_prepQueryFilesByParent->executeQuery();
  while( p_prepQueryFilesByParent->next() ) {
    entry_t* entry = new entry_t;
    entry->type = entry_t::file;
    entry->state = entry_t::entryUnknown;
    entry->id = p_prepQueryFilesByParent->getUInt(1);
    entry->parent = id;
    entry->name = p_prepQueryFilesByParent->getString(2);
    entry->size = p_prepQueryFilesByParent->getUInt64(3);
    entry->mtime = p_prepQueryFilesByParent->getUInt(4);
    entry->hash = p_prepQueryFilesByParent->getString(5);
    entryCache.push_back(entry);
    LOG(logDebug) << "cache: got file id " << entry->id << " parent " << entry->parent << " name " << entry->name << " size " << entry->size << " mtime " << entry->mtime;
  }
  p_prepQueryFilesByParent->release();
}

void worker::clearDatabase() {
  if( !p_databaseInitialized )
    initDatabase();
  if (p_dryRun)
    return;

  query("DROP TABLE "+p_fileTable);
  query("DROP TABLE "+p_directoryTable);
  LOG(logWarning) << "Database tables dropped, data is now gone";
  p_databaseInitialized = false;
}

void worker::deleteDirectory(uint32_t id) { //completely delete directory "id" including all subdirs/files
  if( !p_databaseInitialized )
    initDatabase();
  LOG(logDebug) << "deleting directory id " << id;
  p_prepQueryDirsByParent->setUInt(1,id);
  p_prepQueryDirsByParent->executeQuery();
  vector<uint32_t> childIds( p_prepQueryDirsByParent->rowsCount() );
  vector<uint32_t>::iterator childIdsIt = childIds.begin();
  while( p_prepQueryDirsByParent->next() && childIdsIt != childIds.end() ) {
    *childIdsIt = p_prepQueryDirsByParent->getUInt(1);
    childIdsIt++;
  }
  p_prepQueryDirsByParent->release();
  LOG(logDebug) << "got " << childIds.size() << " children of directory " << id;
  if (!p_dryRun) {
    p_prepDeleteFiles->setUInt(1,id);
    p_prepDeleteFiles->execute(); //now, delete every file in this directory
    p_prepDeleteDir->setUInt(1,id);
    p_prepDeleteDir->execute(); //finally, delete this directory
  }
  for( vector<uint32_t>::iterator it = childIds.begin(); it != childIds.end(); it++ )
    deleteDirectory( *it );
}

void worker::deleteFile(uint32_t id) {
  if( !p_databaseInitialized )
    initDatabase();
  LOG(logDebug) << "deleting file id " << id;
  if (p_dryRun)
    return;
  p_prepDeleteFile->setUInt(1,id);
  p_prepDeleteFile->execute();
}

uint32_t worker::descendPath(string path, entry_t::type_t type, bool createDirectory) {
  uint32_t pathId = 0;
  if( !p_databaseInitialized )
    initDatabase();
  if( !path.empty() ) {
    LOG(logDetailed) << "Descending into specified path " << path;
    while( !path.empty() ) {
      string pathElement = path.substr(0,path.find('/'));
      LOG(logDebug) << "Getting path element " << pathElement;
      path.erase(0,pathElement.length()+1); //erase pathElement and slash
      if( pathElement.empty() ) //strip leading and multiple slashes
        continue;
      entry_t subEntry = getDirectoryByName(pathElement,pathId);
      if( subEntry.id == 0 ) { //directory not found
        if( type == entry_t::directory ) {
          if( createDirectory )
            subEntry.id = insertDirectory(pathId,pathElement,0,time(0));
          else
            throw("unable to descend path, inexistent directory");
        } else {
          subEntry = getFileByName(pathElement,pathId);
        }
      }
      pathId = subEntry.id;
    }
  }
  return pathId;
}

string worker::errnoString() {
   char* e = strerror(errno);
   return e ? e : "";
}

worker::entry_t worker::getDirectoryById(uint32_t id) {
  entry_t e = { .id = id, .mtime = 0, .name = string(), .parent = 0, .size = 0, .subSize = 0, .state = entry_t::entryUnknown, .type = entry_t::directory, .hash = string() };
  p_prepQueryDirById->setUInt(1,id);
  p_prepQueryDirById->executeQuery();
  if( p_prepQueryDirsByParent->next() ) {
    e.name = p_prepQueryDirsByParent->getString(1);
    e.parent = p_prepQueryDirsByParent->getUInt(2);
    e.size = p_prepQueryDirsByParent->getUInt64(3);
    e.mtime = p_prepQueryDirsByParent->getUInt(4);
  } else if (id == 0) {
    e.name = "<ROOT>";
  }
  p_prepQueryDirsByParent->release();
  return e;
}

worker::entry_t worker::getDirectoryByName(const string& name, uint32_t parent) {
  entry_t e = { .id = 0, .mtime = 0, .name = name, .parent = parent, .size = 0, .subSize = 0, .state = entry_t::entryUnknown, .type = entry_t::directory, .hash = string() };
  p_prepQueryDirByName->setUInt(1,parent);
  p_prepQueryDirByName->setString(2,name);
  p_prepQueryDirByName->executeQuery();
  if( p_prepQueryDirByName->next() ) {
    e.id = p_prepQueryDirByName->getUInt(1);
    e.size = p_prepQueryDirByName->getUInt64(2);
    e.mtime = p_prepQueryDirByName->getUInt(3);
  }
  p_prepQueryDirByName->release();
  return e;
}

worker::entry_t worker::getFileById(uint32_t id) {
  entry_t e = { .id = id, .mtime = 0, .name = string(), .parent = 0, .size = 0, .subSize = 0, .state = entry_t::entryUnknown, .type = entry_t::file, .hash = string() };
  p_prepQueryFileById->setUInt(1,id);
  p_prepQueryFileById->executeQuery();
  if( p_prepQueryFileById->next() ) {
    e.name = p_prepQueryFileById->getString(1);
    e.parent = p_prepQueryFileById->getUInt(2);
    e.size = p_prepQueryFileById->getUInt64(3);
    e.mtime = p_prepQueryFileById->getUInt(4);
    e.hash = p_prepQueryFileById->getString(5);
  }
  p_prepQueryFileById->release();
  return e;
}

worker::entry_t worker::getFileByName(const string& name, uint32_t parent) {
  entry_t e = { .id = 0, .mtime = 0, .name = name, .parent = parent, .size = 0, .subSize = 0, .state = entry_t::entryUnknown, .type = entry_t::file, .hash = string() };
  p_prepQueryFileByName->setUInt(1,parent);
  p_prepQueryFileByName->setString(2,name);
  p_prepQueryFileByName->executeQuery();
  if( p_prepQueryFileByName->next() ) {
    e.id = p_prepQueryFileByName->getUInt(1);
    e.size = p_prepQueryFileByName->getUInt64(2);
    e.mtime = p_prepQueryFileByName->getUInt(3);
    e.hash = p_prepQueryFileByName->getString(4);
  }
  p_prepQueryFileByName->release();
  return e;
}

bool worker::getForceHashing() const {
  return p_forceHashing;
}

const worker::statistics& worker::getStatistics() const {
  return p_statistics;
}

void worker::hashFile(entry_t* entry, const string& path) const {
  if( !p_hasher )
    return;
  Hasher::hashStatus_t status = p_hasher->hash(path, entry->hash);
  if( status != Hasher::hashSuccess ) {
    LOG(logError) << "Failed to hash entry " << entry->id;
    entry->hash.clear();
  }
}

void worker::hashCheck(const string& path, uint32_t parent) {
  if( !p_databaseInitialized )
    initDatabase();

  struct stat entryStat;
  vector<entry_t*> entryCache;
  LOG(logDebug) << "fetching directory entries from db for caching";
  cacheDirectoryEntriesFromDB(parent, entryCache);

  for( vector<entry_t*>::iterator it = entryCache.begin(); it != entryCache.end(); it++ ) {
    string subpath = path+"/"+(*it)->name;
    if( (*it)->type == entry_t::file ) {
      LOG(logDebug) << "start hashing file " << subpath;
      string dbHash = (*it)->hash;
      transform(dbHash.begin(), dbHash.end(), dbHash.begin(), ::tolower);
      if( dbHash.length() > 0 ) {
        if( stat(subpath.c_str(), &entryStat) == 0 ) {
          hashFile(*it, subpath);
          if( (*it)->hash.length() ) { //skip empty hash results, error has been reported by hashFile
            if( (*it)->hash != dbHash )
              LOG(logError) << "Hash FAILED for file " << subpath << ": Expected " << dbHash << ", got " << (*it)->hash;
            else
              LOG(logInfo) << "Hash OK: " << subpath;
          }
        } else
          LOG(logError) << "Hash FAILED, file does not exist: " << subpath;
      } else
        LOG(logWarning) << "Database does not contain a hash for " << subpath;
      p_statistics.files++;
    } else {
      LOG(logDetailed) << "Entering subdirectory " << subpath;
      hashCheck(subpath, (*it)->id);
      p_statistics.directories++;
    }
    if (!p_run) //break loop on global abort condition
      break;
  }
}

void worker::initDatabase() {
  LOG(logDebug) << "create tables if not exists"; //create database tables in case they do not exist
  if (p_dryRun)
    return;
  query("CREATE TABLE IF NOT EXISTS "+p_directoryTable+" "
        "(id INT UNSIGNED NOT NULL AUTO_INCREMENT KEY, "
        "name VARCHAR(255) NOT NULL, "
        "parent INT UNSIGNED DEFAULT NULL, "
        "size BIGINT UNSIGNED, "
        "date DATETIME DEFAULT NULL, "
        "INDEX(parent)) "
        "DEFAULT CHARACTER SET utf8 "
        "COLLATE utf8_bin"); //utf8_bin collation against errors with umlauts, e.g. two directories named "Moo" and "Möo"
  query("CREATE TABLE IF NOT EXISTS "+p_fileTable+" "
        "(id INT UNSIGNED NOT NULL AUTO_INCREMENT KEY,"
        "name VARCHAR(255) NOT NULL, "
        "parent INT UNSIGNED DEFAULT NULL, "
        "size BIGINT UNSIGNED, "
        "date DATETIME DEFAULT NULL, "
        "hash VARCHAR(40) DEFAULT NULL, " //use VARCHAR instead of BINARY due to TTH hash support (encoded 39chars base32), md5 and sha1 are encoded hex
        "INDEX(parent)) "
        "DEFAULT CHARACTER SET utf8 "
        "COLLATE utf8_bin"); //utf8_bin collation against errors with umlauts, e.g. two files named "Moo" and "Möo"

  prepareStatements();

  resetStatistics();

  p_databaseInitialized = true;
}

void worker::databaseReconnected() {
  prepareStatements();
}

void worker::prepareStatements() {
  LOG(logDebug) << "preparing statements";

  if (p_prepQueryFileById)
    p_prepQueryFileById->reprepare();
  else
    p_prepQueryFileById = PreparedStatementWrapper::create(this, "SELECT name,parent,size,UNIX_TIMESTAMP(date),hash FROM "+p_fileTable+" WHERE id=?");

  if( p_prepQueryFileByName)
    p_prepQueryFileByName->reprepare();
  else
    p_prepQueryFileByName = PreparedStatementWrapper::create(this, "SELECT id,size,UNIX_TIMESTAMP(date),hash FROM "+p_fileTable+" WHERE parent=? AND name=?");

  if( p_prepQueryFilesByParent)
    p_prepQueryFilesByParent->reprepare();
  else
    p_prepQueryFilesByParent = PreparedStatementWrapper::create(this, "SELECT id,name,size,UNIX_TIMESTAMP(date),hash FROM "+p_fileTable+" WHERE parent=?");

  if( p_prepInsertFile)
    p_prepInsertFile->reprepare();
  else
    p_prepInsertFile = PreparedStatementWrapper::create(this, "INSERT INTO "+p_fileTable+" (name,parent,size,date,hash) VALUES (?, ?, ?, FROM_UNIXTIME(?), ?)");

  if( p_prepUpdateFile)
    p_prepUpdateFile->reprepare();
  else
    p_prepUpdateFile = PreparedStatementWrapper::create(this, "UPDATE "+p_fileTable+" SET size=?, date=FROM_UNIXTIME(?), hash=? WHERE id=?");

  if( p_prepDeleteFile)
    p_prepDeleteFile->reprepare();
  else
    p_prepDeleteFile = PreparedStatementWrapper::create(this, "DELETE FROM "+p_fileTable+" WHERE id=?");

  if( p_prepDeleteFiles)
    p_prepDeleteFiles->reprepare();
  else
    p_prepDeleteFiles = PreparedStatementWrapper::create(this, "DELETE FROM "+p_fileTable+" WHERE parent=?");

  if( p_prepQueryDirById)
    p_prepQueryDirById->reprepare();
  else
    p_prepQueryDirById = PreparedStatementWrapper::create(this, "SELECT name,parent,size,UNIX_TIMESTAMP(date) FROM "+p_directoryTable+" WHERE id=?");

  if( p_prepQueryDirByName)
    p_prepQueryDirByName->reprepare();
  else
    p_prepQueryDirByName = PreparedStatementWrapper::create(this, "SELECT id,size,UNIX_TIMESTAMP(date) FROM "+p_directoryTable+" WHERE parent=? AND name=?");

  if( p_prepQueryDirsByParent)
    p_prepQueryDirsByParent->reprepare();
  else
    p_prepQueryDirsByParent = PreparedStatementWrapper::create(this, "SELECT id,name,size,UNIX_TIMESTAMP(date) FROM "+p_directoryTable+" WHERE parent=?");

  if (p_prepQueryParentOfDir)
    p_prepQueryParentOfDir->reprepare();
  else
    p_prepQueryParentOfDir = PreparedStatementWrapper::create(this, "SELECT parent FROM "+p_directoryTable+" WHERE id=?");

  if( p_prepInsertDir)
    p_prepInsertDir->reprepare();
  else
    p_prepInsertDir = PreparedStatementWrapper::create(this, "INSERT INTO "+p_directoryTable+" (name,parent,size,date) VALUES (?, ?, ?, FROM_UNIXTIME(?))");

  if( p_prepUpdateDir)
    p_prepUpdateDir->reprepare();
  else
    p_prepUpdateDir = PreparedStatementWrapper::create(this, "UPDATE "+p_directoryTable+" SET size=?, date=FROM_UNIXTIME(?) WHERE id=?");

  if( p_prepDeleteDir)
    p_prepDeleteDir->reprepare();
  else
    p_prepDeleteDir = PreparedStatementWrapper::create(this, "DELETE FROM "+p_directoryTable+" WHERE id=?");

  if( p_prepLastInsertID)
    p_prepLastInsertID->reprepare();
  else
    p_prepLastInsertID = PreparedStatementWrapper::create(this, "SELECT LAST_INSERT_ID()");
}

void worker::inheritProperties(entry_t* parent, const entry_t* entry) const {
  if( p_inheritSize )
    parent->subSize += entry->size; //do not flag update yet, total sum is not yet known
  if( p_inheritMTime && parent->mtime < entry->mtime ) {
    parent->mtime = entry->mtime;
    parent->state = entry_t::entryPropertiesChanged; //flag parent to update
  }
}

uint32_t worker::insertDirectory(uint32_t parent, const string& name, uint64_t size, time_t mtime) {
  LOG(logDebug) << "inserting dir " << name << " size " << size << " mtime " << mtime << " parent " << parent;
  if (p_dryRun)
    return ~0;
  p_prepInsertDir->setString(1,name);
  p_prepInsertDir->setUInt(2,parent);
  p_prepInsertDir->setUInt64(3,size);
  p_prepInsertDir->setUInt(4,mtime);
  p_prepInsertDir->execute();

  uint32_t id = 0;
  p_prepLastInsertID->executeQuery();
  if( p_prepLastInsertID->next() )
    id = p_prepLastInsertID->getUInt(1);
  else
    LOG(logError) << "Insert statement failed for " << name;
  p_prepLastInsertID->release();
  return id;
}

uint32_t worker::insertFile(uint32_t parent, const string& name, uint64_t size, time_t mtime, const string& hash) {
  LOG(logDebug) << "inserting file " << name << " size " << size << " mtime " << mtime << " hash " << hash << " parent " << parent;
  if (p_dryRun)
    return ~0;
  p_prepInsertFile->setString(1,name);
  p_prepInsertFile->setUInt(2,parent);
  p_prepInsertFile->setUInt64(3,size);
  p_prepInsertFile->setUInt(4,mtime);
  if( hash.length() )
    p_prepInsertFile->setString(5,hash);
  else
    p_prepInsertFile->setNull(5,0);
  p_prepInsertFile->execute();

  uint32_t id = 0;
  p_prepLastInsertID->executeQuery();
  if( p_prepLastInsertID->next() )
    id = p_prepLastInsertID->getUInt(1);
  else
    LOG(logError) << "Insert statement failed for " << name;
  p_prepLastInsertID->release();
  return id;
}

void worker::parseDirectory(const string& path, uint32_t id) {
  if( !p_databaseInitialized )
    initDatabase();

  entry_t e = getDirectoryById(id);
  parseDirectory(path, &e);
  if( e.id != 0 && e.state == entry_t::entryPropertiesChanged ) //don't write a directory id 0 (no fakepath)
    updateDirectory(e.id, e.size, e.mtime);
}

void worker::parseDirectory(const string& path, entry_t* ownEntry) {
  if( !p_databaseInitialized )
    initDatabase();

  DIR* dir; //directory pointer
  struct dirent* dirEntry; //directory entry
  struct stat64 dirEntryStat; //directory's stat
  vector<entry_t*> entryCache;

  LOG(logDetailed) << "Processing directory " << path;

  dir = opendir(path.c_str());
  if( dir == NULL ) {
    LOG(logError) << "failed to read directory " << path << ": " << errnoString();
    return;
  }

  LOG(logDebug) << "fetching directory entries from db for caching";
  cacheDirectoryEntriesFromDB(ownEntry->id, entryCache);

  while( p_run && ( dirEntry = readdir(dir) ) ) { //readdir returns NULL when "end" of directory is reached
    if( strcmp(dirEntry->d_name,".") == 0 || strcmp(dirEntry->d_name,"..") == 0 ) //don't process . and .. for obvious reasons
      continue;
    string dirEntryPath = path + '/' + dirEntry->d_name;
    LOG(logDebug) << "processing dirEntry " << dirEntryPath;
    if( stat64(dirEntryPath.c_str(), &dirEntryStat) ) {
      LOG(logError) << "stat() on " << dirEntryPath << " failed: " << errnoString();
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
      entry->id = 0;
      entry->parent = ownEntry->id;
      entry->name = dirEntry->d_name;
      entry->mtime = dirEntryStat.st_mtime;
      entry->size = dirEntryStat.st_size;
      entry->state = entry_t::entryNew;
      if( S_ISDIR(dirEntryStat.st_mode) ) {
        entry->subSize = dirEntryStat.st_size;
        entry->type = entry_t::directory;
      } else {
        entry->subSize = 0;
        entry->type = entry_t::file;
        hashFile(entry, dirEntryPath); //hash new files if enabled
      }
      entryCache.push_back(entry);
    } else { //entry is in db, check for changes
      if( entry->type == entry_t::directory )
        entry->subSize = dirEntryStat.st_size;
      else {
        entry->subSize = 0;
        if( entry->size != (uint64_t)dirEntryStat.st_size ) {
          entry->size = dirEntryStat.st_size;
          entry->state = entry_t::entryPropertiesChanged; //only flag files for update, decision on directories will be made after parsing
        }
      }
      if( entry->mtime != dirEntryStat.st_mtime ) {
        entry->mtime = dirEntryStat.st_mtime;
        entry->state = entry_t::entryPropertiesChanged;
      }
      //if hasher is enabled and properties are changed or no hash is calculated yet or hashing is forced, rehash file
      if( p_hasher && entry->type == entry_t::file && (entry->state == entry_t::entryPropertiesChanged || entry->hash.length() == 0 || p_forceHashing) ) {
        hashFile(entry, dirEntryPath);
        entry->state = entry_t::entryPropertiesChanged; //force property update
      }
      if( entry->state == entry_t::entryUnknown ) //if state is not entryPropertiesChanged, flag it as correct
        entry->state = entry_t::entryOk;
    }

    if( entry->type == entry_t::file )
      p_statistics.files++;
    else
      p_statistics.directories++;
  }
  closedir(dir);

  processChangedEntries(entryCache, ownEntry); //add new files, also insert directories (but not yet mtime/size)
  for( vector<entry_t*>::iterator it = entryCache.begin(); it != entryCache.end(); ) { //we do not need any file entry_t anymore, just keep directories to lower the recursion's memory footprint
    if( (*it)->type != entry_t::directory || (*it)->state == entry_t::entryDeleted ) {
      delete *it;
      it = entryCache.erase(it);
    } else
      it++;
  }
  for( vector<entry_t*>::iterator it = entryCache.begin(); it != entryCache.end(); it++ ) { //only directories left
    parseDirectory(path + '/' + (*it)->name, *it);
    inheritProperties(ownEntry, *it); //copies size and mtime info (size to subSize for later comparison)
    if (!p_run) //break loop on global abort condition
      break;
  }
  processChangedEntries(entryCache, ownEntry);
  for( vector<entry_t*>::iterator it = entryCache.begin(); it != entryCache.end(); it++ ) //we do not need any file entry_t anymore, just keep directories
    delete *it; //do not have to call entryCache::erase, it will be deleted anyway

  LOG(logDebug) << "dir " << ownEntry->name << " finished ownEntry->subSize " << ownEntry->subSize << " ownEntry->size " << ownEntry->size;
  if( ownEntry->subSize != ownEntry->size ) {
    ownEntry->size = ownEntry->subSize;
    ownEntry->state = entry_t::entryPropertiesChanged;
  }

  LOG(logDebug) << "leaving directory " << path;
}

void worker::printTree(uint32_t parent, const string& path) {
  if( !p_databaseInitialized )
    initDatabase();

  vector<entry_t*> entryCache;
  LOG(logDebug) << "fetching directory entries from db for caching";
  cacheDirectoryEntriesFromDB(parent, entryCache);

  for( vector<entry_t*>::iterator it = entryCache.begin(); it != entryCache.end(); it++ ) {
    string subpath = path+"/"+(*it)->name;
    if( (*it)->type == entry_t::file ) {
      if (options::getInstance().count("print-sums"))
        cout << (*it)->hash << "  ";
      cout << subpath << endl;
      p_statistics.files++;
    } else {
      LOG(logDetailed) << "Entering subdirectory " << subpath;
      printTree((*it)->id, subpath);
      p_statistics.directories++;
    }
    if (!p_run) //break loop on global abort condition
      break;
  }
}

void worker::processChangedEntries(vector<entry_t*>& entries, entry_t* parentEntry) {
  if (!p_run)
    return;
  vector<entry_t*>::iterator it = entries.begin();
  while( it != entries.end() ) {

    //handle directories
    if( (*it)->type == entry_t::directory )
      switch( (*it)->state ) {
        case entry_t::entryOk : {
          break;
        }
        case entry_t::entryPropertiesChanged : {
          LOG(logInfo) << "Updating directory \"" << (*it)->name << '\"';
          updateDirectory( (*it)->id, (*it)->size, (*it)->mtime );
          break;
        }
        case entry_t::entryNew : {
          LOG(logInfo) << "Inserting directory \"" << (*it)->name << '\"';
          (*it)->id = insertDirectory( (*it)->parent, (*it)->name, (*it)->size, (*it)->mtime );
          (*it)->state = entry_t::entryOk;
          break;
        }
        case entry_t::entryUnknown : //continue to entryDeleted
        case entry_t::entryDeleted : {
          LOG(logInfo) << "Dropping directory \"" << (*it)->name << '\"';
          deleteDirectory( (*it)->id );
          (*it)->state = entry_t::entryDeleted;
          break;
        }
        default : {
          LOG(logError) << "bug: unhandled entry_t::state";
          break;
        }
      }

    //handle files
    if( (*it)->type == entry_t::file )
      switch( (*it)->state ) {
        case entry_t::entryOk : {
          inheritProperties(parentEntry, *it);
          break;
        }
        case entry_t::entryPropertiesChanged : {
          LOG(logInfo) << "Updating file \"" << (*it)->name << '\"';
          updateFile( (*it)->id, (*it)->size, (*it)->mtime, (*it)->hash );
          inheritProperties(parentEntry, *it);
          break;
        }
        case entry_t::entryNew : {
          LOG(logInfo) << "Inserting file \"" << (*it)->name << '\"';
          (*it)->id = insertFile( (*it)->parent, (*it)->name, (*it)->size, (*it)->mtime, (*it)->hash );
          inheritProperties(parentEntry, *it);
          break;
        }
        case entry_t::entryUnknown : //continue to entryDeleted
        case entry_t::entryDeleted : {
          LOG(logInfo) << "Dropping file \"" << (*it)->name << '\"';
          deleteFile( (*it)->id );
          break;
        }
        default : {
          LOG(logError) << "bug: unhandled entry_t::state";
          break;
        }
      }

    it++;
  }
}

//TODO use readPath in parseDirectory?
worker::entry_t worker::readPath(const string& path) {
  LOG(logDebug) << "reading path " << path;

  struct stat64 entryStat; //entry's stat
  entry_t entry = { .id = 0, .mtime = 0, .name = string(), .parent = 0, .size = 0, .subSize = 0, .state = entry_t::entryUnknown, .type = entry_t::any, .hash = string() };

  if( stat64(path.c_str(), &entryStat) ) {
    LOG(logError) << "stat64() on " << path << " failed: " << errnoString();
    return entry;
  }

  entry.name = path.substr( path.find_last_of('/') );
  entry.size = entryStat.st_size;
  entry.mtime = entryStat.st_mtime;
  if( S_ISDIR(entryStat.st_mode) )
    entry.type = entry_t::directory;
  else
    entry.type = entry_t::file;
  entry.state = entry_t::entryOk;

  //TODO hashing, with special care about inotify

  return entry;
}

void worker::removeWatches(uint32_t id) {
  p_prepQueryDirsByParent->setUInt(1,id);
  p_prepQueryDirsByParent->executeQuery();
  vector<uint32_t> cache;
  while( p_prepQueryDirsByParent->next() )
    cache.push_back(p_prepQueryDirsByParent->getUInt(1));
  p_prepQueryDirsByParent->release();
  for( vector<uint32_t>::iterator it = cache.begin(); it != cache.end(); it++ )
    removeWatches(*it);
  LOG(logDebug) << "removing watch of id " << id;
  for(map< int, pair<uint32_t,string> >::iterator it = p_watches.begin(); it != p_watches.end(); it++)
    if( it->second.first == id )
      inotify_rm_watch(p_watchDescriptor, it->first);
}

void worker::resetStatistics() {
  p_statistics.files = 0;
  p_statistics.directories = 0;
}

void worker::setConnection(MYSQL* dbConnection) {
  p_connection = dbConnection;
  p_databaseInitialized = false;
}

MYSQL* worker::getConnection() const {
  return p_connection;
}

void worker::setForceHashing(bool force) {
  p_forceHashing = force;
}

void worker::setHasher(Hasher* hasher) {
  p_hasher = hasher;
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

void worker::setupWatches(const string& path, uint32_t id) {
  p_prepQueryDirsByParent->setUInt(1,id);
  p_prepQueryDirsByParent->executeQuery();
  vector< pair<uint32_t, string> > cache;
  while( p_prepQueryDirsByParent->next() )
    cache.push_back( make_pair(p_prepQueryDirsByParent->getUInt(1), p_prepQueryDirsByParent->getString(2)) );
  p_prepQueryDirsByParent->release();
  for( vector< pair<uint32_t, string> >::iterator it = cache.begin(); it != cache.end(); it++ )
    setupWatches(path+'/'+it->second, it->first);
  LOG(logDetailed) << "Setting up watch for \"" << path << "\" (id " << id << ')';
  int dirWatchDescriptor = inotify_add_watch( p_watchDescriptor, path.c_str(), IN_CLOSE_WRITE | IN_MOVED_FROM | IN_MOVED_TO | IN_CREATE | IN_DELETE | IN_ONLYDIR);
  if( dirWatchDescriptor != 0 )
    p_watches[dirWatchDescriptor] = make_pair(id,path);
  else
    LOG(logError) << "Unable to setup watch for id " << id << " with path \"" << path << '\"';
}

void worker::updateDirectory(uint32_t id, uint64_t size, time_t mtime) {
  LOG(logDebug) << "updating dir id " << id << " size " << size << " mtime " << mtime;
  if (p_dryRun)
    return;
  p_prepUpdateDir->setUInt64(1,size);
  p_prepUpdateDir->setUInt(2,mtime);
  p_prepUpdateDir->setUInt(3,id);
  p_prepUpdateDir->execute();
}

void worker::updateFile(uint32_t id, uint64_t size, time_t mtime, const string& hash) {
  LOG(logDebug) << "updating file id " << id << " size " << size << " mtime " << mtime << " hash " << hash;
  if (p_dryRun)
    return;
  p_prepUpdateFile->setUInt64(1,size);
  p_prepUpdateFile->setUInt(2,mtime);
  if( hash.length() )
    p_prepUpdateFile->setString(3,hash);
  else
    p_prepUpdateFile->setNull(3,0);
  p_prepUpdateFile->setUInt(4,id);
  p_prepUpdateFile->execute();
}

void worker::updateTreeProperties(uint32_t firstParent, int64_t sizeDiff, time_t newMTime) {
  LOG(logDetailed) << "Updating directory id " << firstParent << " recursively";
  if (p_dryRun)
    return;
  entry_t e = getDirectoryById(firstParent);
  if( p_inheritMTime && e.mtime < newMTime )
    e.mtime = newMTime;
  updateDirectory(firstParent, e.size + sizeDiff, e.mtime);
  if( e.parent != 0 )
    updateTreeProperties(e.parent, sizeDiff, newMTime);
}

//verifies the complete tree, deletes orphaned entries
//resource intensive, traces every entry up to the root or a cached (valid) parent id
void worker::verifyTree() {
  if( !p_databaseInitialized )
    initDatabase();

  list<uint32_t>* idCache = new list<uint32_t>; //contains valid ids that have a connection to parent 0
  list<uint32_t>::iterator cacheIterator;

  LOG(logDetailed) << "Verifying directories";
  PreparedStatementWrapper* stmt = PreparedStatementWrapper::create(this, "SELECT id,parent FROM "+p_directoryTable);
  stmt->executeQuery();
  while( p_run && stmt->next() ) { //loop through all directories
    uint32_t id = stmt->getUInt(1);
    uint32_t parent = stmt->getUInt(2);
    uint32_t tempId = id; //id-parent pairs used to trace up until they are either found in idCache or tempParentId gets zero
    uint32_t tempParentId = parent;
    list<uint32_t> tempIdCache;
    LOG(logDebug) << "Verify: id " << id << " parent " << parent;
    if( id == parent ) {
      LOG(logWarning) << "id " << id << " is it's own parent. Deleting!";
      tempId = 0; //indicate failure
      tempParentId = 0;
      if (!p_dryRun) {
        stringstream ss;
        ss << "DELETE FROM " << p_directoryTable << " WHERE parent=" << id;
        query(ss.str());
        p_prepDeleteFiles->setUInt(1,id); //delete all files with parent "id"
        p_prepDeleteFiles->execute();
      }
    }
    tempIdCache.push_back(id); //pre-cache id in case it may be valid
    //now we check if we can find the given parent of id
    while( p_run && tempParentId != 0 && find(idCache->begin(), idCache->end(), tempParentId) == idCache->end() ) {
      LOG(logDebug) << "Ancestor: id " << tempId << " parent " << tempParentId;
      p_prepQueryParentOfDir->setUInt(1,tempParentId);
      p_prepQueryParentOfDir->executeQuery();
      if( p_prepQueryParentOfDir->next() ) {
        LOG(logDebug) << "found ancestor id " << tempParentId << " of id " << id << " in database, continueing trace";
        tempId = tempParentId;
        tempParentId = p_prepQueryParentOfDir->getUInt(1);
        tempIdCache.push_back(tempId); //tempId was found in the database, so it's a valid parent (if we are able to complete the trace)
      } else {
        LOG(logWarning) << "Parent " << tempParentId << " of directory " << tempId << " does not exist. Deleting that subtree!";
        deleteDirectory(tempId); //we do not have to remove incorrectly cached ids though we did not cache invalid ones
        tempId = 0; //indicate failure
        tempParentId = 0;
      }
      p_prepQueryParentOfDir->release();
      if( tempParentId == id ) { //loop detection
        LOG(logWarning) << "Detected loop between id " << tempId << " and id " << id;
        deleteDirectory(id); //will delete a loop reliably
        tempId = 0; //indicate failure
        tempParentId = 0;
      }
    }
    LOG(logDebug) << "Traceback complete, tempId " << tempId << " tempParentId " << tempParentId;
    if( tempId != 0 ) //only cache if valid
      idCache->merge(tempIdCache); //cache all temporary ids as the trace is okay
    p_statistics.directories++;
  }
  delete stmt;

  LOG(logDetailed) << "Verifying files";
  stmt = PreparedStatementWrapper::create(this, "SELECT id,parent FROM "+p_fileTable);
  stmt->executeQuery();
  while( p_run && stmt->next() ) {
    uint32_t id = stmt->getUInt(1);
    uint32_t parent = stmt->getUInt(2);
    if( parent == 0 || find(idCache->begin(), idCache->end(), parent) == idCache->end() ) {
      LOG(logWarning) << "Parent " << parent << " of file " << id << " does not exist. Deleting that file";
      if (!p_dryRun) {
        p_prepDeleteFile->setUInt(1,id);
        p_prepDeleteFile->execute();
      }
    }
    p_statistics.files++;
  }
  delete stmt;
  delete idCache;
}

//TODO signal handler to clean up on ctrl+c/SIGTERM
void worker::watch(const string& path, uint32_t id) {
  if( !p_databaseInitialized )
    initDatabase();

  p_watches.clear();
  LOG(logDebug) << "initializing inotify";
  p_watchDescriptor = inotify_init();
  LOG(logInfo) << "Setting up watches";
  setupWatches(path,id);

  LOG(logInfo) << "Setup complete, waiting for events...";
  const int eventSize = sizeof(struct inotify_event) + NAME_MAX + 1;

  struct pollfd fds = { .fd = p_watchDescriptor, .events = POLLIN, .revents = 0 };

  while( p_run ) {
    poll(&fds, 1, 1000); //poll for new events every second
    if (!(fds.revents & POLLIN))
      continue;
    char* buffer = new char[eventSize];
    int len = read(p_watchDescriptor, buffer, eventSize);
    int offset = 0;
    while( offset < len ) {
      struct inotify_event* event = (struct inotify_event*)(buffer+offset);
      switch( event->mask ) {
        case IN_ATTRIB : {
          LOG(logDebug) << "got inotify event IN_ATTRIB for file \"" << event->name << "\" cookie " << event->cookie << " wd " << event->wd << " dir " << p_watches[event->wd].first;
          break; //do not handle since touching a file also evokes IN_CLOSE_WRITE
        }
        case IN_ATTRIB | IN_ISDIR : { //directory's mtime changed
          LOG(logDebug) << "got inotify event IN_ATTRIB for dir \"" << event->name << "\" cookie " << event->cookie << " wd " << event->wd << " dir " << p_watches[event->wd].first;
          const pair<uint32_t,string> p = p_watches[event->wd];
          const string path = p.second + '/' + event->name;
          entry_t fsEntry = readPath(path);
          if( fsEntry.state == entry_t::entryUnknown ) { //readPath failed
            LOG(logError) << "failed to read path \"" << path << '\"';
            break;
          }
          entry_t dbEntry = getDirectoryByName(event->name, p.first);
          if( dbEntry.id == 0 ) {
            LOG(logWarning) << "Modified directory " << event->name << " was not yet in database, fixing.";
            dbEntry.id = insertDirectory(p.first, fsEntry.name, fsEntry.size, fsEntry.mtime);
            parseDirectory(path, &dbEntry);
            updateDirectory(dbEntry.id, dbEntry.size, dbEntry.mtime);
            updateTreeProperties(p.first, dbEntry.size, dbEntry.mtime);
          } else {
            LOG(logInfo) << "Updating directory " << event->name;
            updateDirectory(dbEntry.id, dbEntry.size, fsEntry.mtime);
            updateTreeProperties(p.first, 0, fsEntry.mtime); //no size change intended
          }
          break;
        }
        case IN_CREATE : {
          LOG(logDebug) << "got inotify event IN_CREATE for file \"" << event->name << "\" cookie " << event->cookie << " wd " << event->wd << " dir " << p_watches[event->wd].first;
          const pair<uint32_t,string> p = p_watches[event->wd];
          const string path = p.second + '/' + event->name;
          entry_t e = readPath( path );
          if( e.state == entry_t::entryOk ) { //readPath successful
            //insert a blank hash because the file may not be written out completely
            //hashing will be done by IN_CLOSE_WRITE when the file is closed after writing
            insertFile(p.first, event->name, e.size, e.mtime, string());
            updateTreeProperties(p.first, e.size, e.mtime);
          } else
            LOG(logError) << "failed to read path \"" << path << '\"';
          break;
        }
        case IN_CREATE | IN_ISDIR : {
          LOG(logDebug) << "got inotify event IN_CREATE for dir \"" << event->name << "\" cookie " << event->cookie << " wd " << event->wd << " dir " << p_watches[event->wd].first;
          const pair<uint32_t,string> p = p_watches[event->wd];
          const string path = p.second + '/' + event->name;
          entry_t e = readPath( path );
          if( e.state == entry_t::entryOk ) { //readPath successful
            e.id = insertDirectory(p.first, event->name, e.size, e.mtime);
            updateTreeProperties(p.first, e.size, e.mtime);
            setupWatches(path, e.id);
          } else
            LOG(logError) << "failed to read path \"" << path << '\"';
          break;
        }
        case IN_CLOSE_WRITE : { //covers newly created files as well as modified existing files
          LOG(logDebug) << "got inotify event IN_CLOSE_WRITE for file \"" << event->name << "\" cookie " << event->cookie << " wd " << event->wd << " dir " << p_watches[event->wd].first;
          const pair<uint32_t,string> p = p_watches[event->wd];
          const string path = p.second + '/' + event->name;
          entry_t fsEntry = readPath(path);
          if( fsEntry.state == entry_t::entryUnknown ) { //readPath failed
            LOG(logError) << "failed to read path \"" << path << '\"';
            break;
          }
          entry_t dbEntry = getFileByName(event->name, p.first);
          hashFile(&dbEntry, path); //in any case, hash modified (or inexistent) file if enabled
          if( dbEntry.id == 0 ) {
            //new files should have been created by IN_CREATE, now we should only need to update size/mtime/hash
            LOG(logWarning) << "Modified file " << event->name << " was not yet in database, fixing.";
            insertFile(p.first, fsEntry.name, fsEntry.size, fsEntry.mtime, dbEntry.hash);
            updateTreeProperties(p.first, fsEntry.size, fsEntry.mtime);
          } else {
            LOG(logInfo) << "Updating file " << event->name;
            updateFile(dbEntry.id, fsEntry.size, fsEntry.mtime, dbEntry.hash);
            updateTreeProperties(p.first, fsEntry.size - dbEntry.size, fsEntry.mtime);
          }
          break;
        }
        case IN_CLOSE_WRITE | IN_ISDIR : {
          LOG(logDebug) << "got inotify event IN_CLOSE_WRITE for dir \"" << event->name << "\" cookie " << event->cookie << " wd " << event->wd << " dir " << p_watches[event->wd].first;
          break; //won't happen, mtime changes are covered by IN_ATTRIB
        }
        case IN_MOVED_TO : {
          LOG(logDebug) << "got inotify event IN_MOVED_TO for file \"" << event->name << "\" cookie " << event->cookie << " wd " << event->wd << " dir " << p_watches[event->wd].first;
          const pair<uint32_t,string> p = p_watches[event->wd];
          const string path = p.second + '/' + event->name;
          entry_t e = readPath( path );
          if( e.state == entry_t::entryOk ) { //readPath successful
            hashFile(&e, path);
            insertFile(p.first, event->name, e.size, e.mtime, e.hash);
            updateTreeProperties(p.first, e.size, e.mtime);
          } else
            LOG(logError) << "failed to read path \"" << path << '\"';
          break;
        }
        case IN_MOVED_TO | IN_ISDIR : {
          LOG(logDebug) << "got inotify event IN_MOVED_TO for dir \"" << event->name << "\" cookie " << event->cookie << " wd " << event->wd << " dir " << p_watches[event->wd].first;
          const pair<uint32_t,string> p = p_watches[event->wd];
          const string path = p.second + '/' + event->name;
          entry_t e = readPath( path );
          if( e.state == entry_t::entryOk ) { //readPath successful
            e.id = insertDirectory(p.first, event->name, e.size, e.mtime);
            parseDirectory(path, &e);
            setupWatches(path, e.id);
            updateDirectory(e.id, e.size, e.mtime);
            updateTreeProperties(p.first, e.size, e.mtime);
          } else
            LOG(logError) << "failed to read path \"" << path << '\"';
          break;
        }
        case IN_MOVED_FROM :
        case IN_DELETE : {
          LOG(logDebug) << "got inotify event IN_DELETE/IN_MOVED_FROM for file \"" << event->name << "\" cookie " << event->cookie << " wd " << event->wd << " dir " << p_watches[event->wd].first;
          const pair<uint32_t,string> p = p_watches[event->wd];
          entry_t e = getFileByName(event->name, p.first);
          if( e.id != 0 ) {
            LOG(logInfo) << "Removing file " << event->name;
            deleteFile(e.id);
            updateTreeProperties(p.first, (int64_t)-1*e.size, 0);
          } else
            LOG(logError) << "Failed to get file \"" << event->name << "\" from db for removal, already deleted.";
          break;
        }
        case IN_MOVED_FROM | IN_ISDIR :
        case IN_DELETE | IN_ISDIR : {
          LOG(logDebug) << "got inotify event IN_DELETE/IN_MOVED_FROM for dir \"" << event->name << "\" cookie " << event->cookie << " wd " << event->wd << " dir " << p_watches[event->wd].first;
          const pair<uint32_t,string> p = p_watches[event->wd];
          entry_t e = getDirectoryByName(event->name, p.first);
          if( e.id != 0 ) {
            removeWatches(e.id);
            LOG(logInfo) << "Removing directory " << event->name;
            deleteDirectory(e.id);
            updateTreeProperties(p.first, (int64_t)-1*e.size, 0);
          } else
            LOG(logError) << "Failed to get directory \"" << event->name << "\" from db for removal, already deleted.";
          break;
        }
        default : {
          LOG(logDebug) << "unhandled inotify event " << event->mask << " for file \"" << event->name << "\" cookie " << event->cookie << " wd " << event->wd << " dir " << p_watches[event->wd].first;
          break;
        }
      }
      offset += sizeof(inotify_event)+event->len;
    }
    delete[] buffer;
  }

  //program cannot reach that point till now
  LOG(logInfo) << "Giving up watches";
  removeWatches(id);
}

void worker::query(const string& query) {
  int ret = mysql_query(p_connection, query.c_str());
  if (ret)
    throw SQLException("mysql_query failed: "+string(mysql_error(p_connection)));
}
