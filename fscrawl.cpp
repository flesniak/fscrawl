/* compile with:
 * g++ -I /usr/include/mysql-connector/ -lmysqlcppconn -lstdc++ -O2 -o fscrawl fscrawl.cpp */

#include <iostream>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <sstream>
#include <ctime>
#include <cerrno>
#include <map>
#include <list>

#include <dirent.h>
#include <sys/stat.h>

#include <cppconn/driver.h>
#include <cppconn/prepared_statement.h>
#include <mysql_connection.h>

//fscrawl uses these tables by default
#define FILES_TABLE "fscrawl_files"
#define DIRECTORIES_TABLE "fscrawl_directories"

#define VERSION "1.42"

using namespace std;

unsigned char verbosity;
unsigned int filecount;
bool use_mtime;
sql::PreparedStatement *prepQueryFile;
sql::PreparedStatement *prepInsertFile;
sql::PreparedStatement *prepQueryDir;
sql::PreparedStatement *prepInsertDir;
sql::PreparedStatement *prepLastInsertID;
sql::PreparedStatement *prepUpdateDirSize;
sql::PreparedStatement *prepUpdateFileSize;
sql::PreparedStatement *prepUpdateDirDate;
sql::PreparedStatement *prepUpdateFileDate;
sql::Statement *stmt;
map<uint32_t,uint32_t> parentIdCache;
//        id,  parent


inline void debug(string message, unsigned char level) {
  if( verbosity >= level )
    cerr << "V" << (uint32_t)level << ": " << message << endl;
}

string errnoString() {
   char * e = strerror(errno);
   return e ? e : "";
}

void deleteDirectory(unsigned int id) { //completely delete directory "id" including all subdirs/files
  stringstream ss;
  ss << "SELECT id FROM "DIRECTORIES_TABLE" WHERE parent=" << id;
  sql::ResultSet *res = stmt->executeQuery(ss.str());
  while( res->next() )
    deleteDirectory( res->getInt(1) ); //first, delete every subdirectory
  ss.str("");
  ss << "DELETE FROM "FILES_TABLE" WHERE parent=" << id;
  stmt->execute(ss.str()); //now, delete every file in this directory
  ss.str("");
  ss << "DELETE FROM "DIRECTORIES_TABLE" WHERE id=" << id;
  stmt->execute(ss.str()); //finally, delete this directory
}

//verifies the complete tree, deletes orphaned entries
//resource intensive, traces every entry up to the root or a cached (valid) parent id
int verifyTree(sql::Connection *con) {
  parentIdCache.clear();
  sql::PreparedStatement *prepGetDirParent = con->prepareStatement("SELECT parent FROM "DIRECTORIES_TABLE" WHERE id=?");

  debug("Checking directories...",3);
  sql::Statement *stmt = con->createStatement();
  sql::ResultSet *res = stmt->executeQuery("SELECT id,parent FROM "DIRECTORIES_TABLE);
  while( res->next() ) { //loop through all directories
    uint32_t id = res->getUInt(1);
    uint32_t parent = res->getUInt(2);
    if( parent == 0 ) //parent 0 is always valid
      continue;
    prepGetDirParent->setUInt(1,parent);
    sql::ResultSet *parentQueryResult = prepGetDirParent->executeQuery();
    if( parentQueryResult->next() ) { //parent exists, cache it - saves us query time on the files later on
      parentIdCache[id] = parent;
      parentIdCache[parent] = parentQueryResult->getUInt(1);
    }
    else {
      stringstream ss;
      ss << "Parent " << parent << " of directory " << id << " does not exist. Deleting that subtree";
      debug(ss.str(),2);
      deleteDirectory(id);
    }
    delete parentQueryResult;
  }
  delete res;

  debug("Directories verified, checking files...",3);
  res = stmt->executeQuery("SELECT id,parent FROM "FILES_TABLE);
  while( res->next() ) {
    uint32_t id = res->getUInt(1);
    uint32_t parent = res->getUInt(2);
    if( parent == 0 || parentIdCache.count(parent) == 0 ) {
      stringstream ss;
      ss << "Parent " << parent << " of file " << id << " does not exist. Deleting that file";
      ss.str("");
      ss << "DELETE FROM "FILES_TABLE" WHERE id=" << id;
      stmt->executeQuery(ss.str());
    }
  }
  delete res;
  delete stmt;
}

//inserts file "name" under "parent" with "size" if not exists and (in any case) returns its id
int addFile(unsigned int parent, string name, uint64_t size, time_t mtime) {
  prepQueryFile->setUInt(1,parent);
  prepQueryFile->setString(2,name);
  unsigned int fileId;
  sql::ResultSet *res = prepQueryFile->executeQuery();
  if( res->next() ) {
    debug("file "+name+" is in db, id "+res->getString(1),3);
    fileId = res->getInt(1);
    if( size != res->getUInt64(2) ) { //if size has changed, update it
      debug("file "+name+" altered size, updating",2);
      prepUpdateFileSize->setUInt64(1,size);
      prepUpdateFileSize->setUInt(2,mtime);
      prepUpdateFileSize->setUInt(3,fileId);
      prepUpdateFileSize->execute();
    }
    if( res->isNull(3) ) {
      prepUpdateFileDate->setUInt(1,mtime);
      prepUpdateFileDate->setUInt(2,fileId);
      prepUpdateFileDate->execute();
    }
  } else {
    debug("file "+name+" is not in db, inserting",2);
    prepInsertFile->setString(1,name);
    prepInsertFile->setUInt(2,parent);
    prepInsertFile->setUInt64(3,size);
    prepInsertFile->setUInt(4,mtime);
    prepInsertFile->execute();
    delete res;
    res = prepLastInsertID->executeQuery();
    if( res->next() )
      fileId = res->getInt(1);
    else {
      cerr << "ERROR: Insert statement failed for " << name << endl;
      exit(5);
    }
  }
  delete res;
  filecount++;
  return fileId;
}

//inserts directory "name" under "parent" if not exists and (in any case) returns its id
int addDirectory(unsigned int parent, string name, time_t mtime) {
  unsigned int dirId;
  prepQueryDir->setUInt(1,parent);
  prepQueryDir->setString(2,name);
  sql::ResultSet *res = prepQueryDir->executeQuery();
  if( res->next() ) {
    debug("directory "+name+" is in db, id "+res->getString(1),3);
    dirId = res->getInt(1);
    if( res->isNull(2) ) {
      prepUpdateDirDate->setUInt(1,mtime);
      prepUpdateDirDate->setUInt(2,dirId);
      prepUpdateDirDate->execute();
    }
  } else {
    debug("directory "+name+" is not in db, inserting",2);
    prepInsertDir->setString(1,name);
    prepInsertDir->setUInt(2,parent);
    prepInsertDir->setUInt(3,mtime);
    prepInsertDir->execute();
    delete res;
    res = prepLastInsertID->executeQuery();
    if( res->next() )
      dirId = res->getInt(1);
    else {
      cerr << "ERROR: Insert statement failed for " << name << endl;
      exit(5);
    }
  }
  delete res;
  return dirId;
}

void deleteDeletedFiles(unsigned int id, vector<unsigned int> *presentFiles, vector<unsigned int> *presentDirectories) {
  stringstream ss;

  debug("deleting orphaned files and directories",3);
  ss << "DELETE FROM "FILES_TABLE" WHERE parent=" << id;
  if( !presentFiles->empty() ) {
    ss << " AND id NOT IN (";
    for(vector<unsigned int>::iterator it = presentFiles->begin(); it != presentFiles->end(); it++) {
      if( it != presentFiles->begin() )
         ss << ",";
      ss << *it;
    }
    ss << ")";
  }
  stmt->execute(ss.str());

  ss.str("");
  ss << "SELECT id FROM "DIRECTORIES_TABLE" WHERE parent=" << id;
  if( !presentDirectories->empty() ) {
    ss << " AND id NOT IN (";
    for(vector<unsigned int>::iterator it = presentDirectories->begin(); it != presentDirectories->end(); it++) {
      if( it != presentDirectories->begin() )
         ss << ",";
      ss << *it;
    }
    ss << ")";
  }
  sql::ResultSet *res = stmt->executeQuery(ss.str());
  while( res->next() )
    deleteDirectory( res->getInt(1) );
}

uint64_t parseDirectory(unsigned int id, string path) { //returns the directory size
  DIR *dirpointer; //directory pointer
  struct dirent *direntry; //directory entry
  struct stat64 dirstat; //directorie's stat
  string subpath; //complete path of one directory member
  unsigned int subid;
  time_t mtime;
  uint64_t dirsize = 0;
  vector<unsigned int> presentFiles;
  vector<unsigned int> presentDirectories;

  dirpointer = opendir(path.c_str());
  if( dirpointer == NULL ) {
    cerr << "ERROR: failed to read directory " << path << ": " << errnoString() << endl;
    return 0;
  }
  debug("parsing directory "+path,3);

  while( ( direntry = readdir(dirpointer) ) ) { //readdir returns NULL when "end" of directory is reached
    if( strcmp(direntry->d_name,".") == 0 || strcmp(direntry->d_name,"..") == 0 ) //don't process . and .. for obvious reasons
      continue;
    string subpath = path + "/" + direntry->d_name;
    if( stat64(subpath.c_str(), &dirstat) ) {
      cerr << "ERROR: stat() on " << subpath << " failed: " << errnoString() << endl;
      continue;
    }
    if( use_mtime )
      mtime = dirstat.st_mtime;
    else
      mtime = time(0);
    if( S_ISDIR(dirstat.st_mode) ) {
      subid = addDirectory(id,direntry->d_name,mtime); //add directory usind id as its parent
      dirsize += parseDirectory(subid,subpath); //parse that subdirectory!
      presentDirectories.push_back(subid); //remember its id for cleanup
    } else {
      subid = addFile(id,direntry->d_name,dirstat.st_size,mtime); //add file using id as its parent and supplying its size
      presentFiles.push_back(subid); //remember its id for cleanup
      dirsize += dirstat.st_size;
    }
  }
  deleteDeletedFiles(id,&presentFiles,&presentDirectories);
  prepUpdateDirSize->setUInt64(1,dirsize);
  prepUpdateDirSize->setUInt(2,id);
  prepUpdateDirSize->execute();
  presentFiles.clear(),
  presentDirectories.clear();
  debug("leaving directory "+path,3);
  closedir(dirpointer);
  return dirsize;
}

void usage() {
  cout << "Usage: fscrawl <MODE> [OPTIONS] <basedir|-c>" << endl;
  cout << "  -h, --help\tDisplay this help and exit" << endl;
  cout << "  -u, --user\tSpecify database user (default: \"root\")" << endl;
  cout << "  -p, --password\tSpecify database user password (default: none)" << endl;
  cout << "  -m, --host\tConnect to host (default: \"localhost\"" << endl;
  cout << "  -d, --database\tDatabase to use (default: \"fscrawl\")" << endl;
  cout << "  -t, --modtime\tUse mtime for the \"date\" column" << endl;
  cout << "  -f, --fakepath\tInstead of having basedir as absolute root directory, parse all files as if they were unter this fakepath" << endl;
  cout << "  -v, --verbose\tIncrease debug level (0x=error,1x=info,2x=debug)" << endl;
  cout << "  -q, --quiet\tDont display statistics after crawling" << endl;
  cout << endl << "The following options may be called without a basedir, then no scanning will be done:" << endl;
  cout << "  -c, --clear\tDelete the tree for this fakepath, others will be kept" << endl;
  cout << "  -C, --clearall\tClear the database - ALL DATA WILL BE DELETED" << endl;
  cout << "  -V, --verify\tVerify the tree structure - takes some time" << endl;
  cout << endl << "fscrawl version "VERSION << endl;
}

int main(int argc, char* argv[]) {
  verbosity = 1; //default, display stats after crawling
  filecount = 0;
  use_mtime = false;
  bool verify = false;
  unsigned char clear = 0;
  string basedir;
  string fakepath;
  string mysql_user("root");
  string mysql_password;
  string mysql_host("localhost");
  string mysql_database("fscrawl");

  debug("parsing command line arguments",3);
  for(int i = 1; i < argc; i++) {
    if( strcmp(argv[i],"-h") == 0 || strcmp(argv[i],"--help") == 0 ) {
      usage();
      exit(1);
    }
    if( strcmp(argv[i],"-v") == 0 || strcmp(argv[i],"--verbose") == 0) {
      verbosity++;
      continue;
    }
    if( strcmp(argv[i],"-vv") == 0 ) {
      verbosity += 2;
      continue;
    }
    if( strcmp(argv[i],"-vvv") == 0 ) {
      verbosity += 3;
      continue;
    }
    if( strcmp(argv[i],"-q") == 0 || strcmp(argv[i],"--quiet") == 0 ) {
      verbosity = 0;
      continue;
    }
    if( strcmp(argv[i],"-q") == 0 || strcmp(argv[i],"--quiet") == 0 ) {
      use_mtime = true;
      continue;
    }
    if( strcmp(argv[i],"-u") == 0 || strcmp(argv[i],"--user") == 0) {
      if( i+1 >= argc ) {
        cerr << "ERROR: expected user" << endl;
        usage();
        exit(1);
      }
      i++;
      mysql_user = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-p") == 0 || strcmp(argv[i],"--password") == 0) {
      if( i+1 >= argc ) {
        cerr << "ERROR: expected password" << endl;
        usage();
        exit(1);
      }
      i++;
      mysql_password = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-m") == 0 || strcmp(argv[i],"--host") == 0) {
      if( i+1 >= argc ) {
        cerr << "ERROR: expected host" << endl;
        usage();
        exit(1);
      }
      i++;
      mysql_host = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-d") == 0 || strcmp(argv[i],"--database") == 0) {
      if( i+1 >= argc ) {
        cerr << "ERROR: expected database" << endl;
        usage();
        exit(1);
      }
      i++;
      mysql_database = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-C") == 0 || strcmp(argv[i],"--clearall") == 0) {
      clear = true;
      continue;
    }
    if( strcmp(argv[i],"-f") == 0 || strcmp(argv[i],"--fakepath") == 0) {
      if( i+1 >= argc ) {
        cerr << "ERROR: expected fakepath" << endl;
        usage();
        exit(1);
      }
      i++;
      fakepath = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-V") == 0 || strcmp(argv[i],"--verify") == 0) {
      verify = true;
      continue;
    }
    if( strncmp(argv[i],"-",1) == 0 ) {
      cerr << "ERROR: unknown argument " << argv[i] << endl;
      usage();
      exit(1);
    }
    if( basedir.empty() )
      basedir = argv[i];
    else {
      cerr << "ERROR: invalid additional argument " << argv[i] << endl;
      usage();
      exit(1);
    }
  }

  if( basedir.empty() ) {
    if( !clear && !verify ) { //if no basedir is given but clear is set, only clear the db and exit
      cout << "ERROR: no basedir given" << endl;
      usage();
      exit(1);
    }
  }
  else
    if( basedir.at(basedir.size()-1) == '/' ) //don't try that on an empty basedir string
      basedir.erase(basedir.size()-1);

  debug("initializing sql driver",3);
  sql::Driver *driver = get_driver_instance();
  sql::Connection *con = driver->connect(mysql_host,mysql_user,mysql_password);
  con->setSchema(mysql_database);
  stmt = con->createStatement();
  if( clear == 2 ) { //complete prune requested
    debug("clearing database, dropping tables - ALL DATA IS NOW GONE",2);
    stmt->execute("DROP TABLE "FILES_TABLE);
    stmt->execute("DROP TABLE "DIRECTORIES_TABLE);
  }
  if( verify ) {
    debug("Launching tree verification",3);
    verifyTree(con);
    debug("Tree verification done",3);
  }
  if( !basedir.empty() ) {
    debug("preparing database",3); //create database tables in case they do not exist
    stmt->execute("CREATE TABLE IF NOT EXISTS "FILES_TABLE
                  "(id INT UNSIGNED NOT NULL AUTO_INCREMENT KEY,"
                  "name VARCHAR(255) NOT NULL,"
                  "parent INT UNSIGNED NOT NULL,"
                  "size BIGINT UNSIGNED,"
                  "date DATETIME DEFAULT NULL,"
                  "INDEX(parent))"
                  "DEFAULT CHARACTER SET utf8 "
                  "COLLATE utf8_bin"); //utf8_bin collation against errors with umlauts, e.g. two files named "Moo" and "Möo"
    stmt->execute("CREATE TABLE IF NOT EXISTS "DIRECTORIES_TABLE
                  "(id INT UNSIGNED NOT NULL AUTO_INCREMENT KEY,"
                  "name VARCHAR(255) NOT NULL,"
                  "parent INT UNSIGNED NOT NULL,"
                  "size BIGINT UNSIGNED,"
                  "date DATETIME DEFAULT NULL,"
                  "INDEX(parent))"
                  "DEFAULT CHARACTER SET utf8 "
                  "COLLATE utf8_bin"); //utf8_bin collation against errors with umlauts, e.g. two directories named "Moo" and "Möo"

    //prepare heavily used mysql functions
    prepQueryFile = con->prepareStatement("SELECT id,size,date FROM "FILES_TABLE" WHERE parent=? AND name=?");
    prepInsertFile = con->prepareStatement("INSERT INTO "FILES_TABLE" (name,parent,size,date) VALUES (?, ?, ?, FROM_UNIXTIME(?))");
    prepUpdateFileSize = con->prepareStatement("UPDATE "FILES_TABLE" SET size=?, date=FROM_UNIXTIME(?) WHERE id=?");
    prepUpdateFileDate = con->prepareStatement("UPDATE "FILES_TABLE" SET date=FROM_UNIXTIME(?) WHERE id=?");

    prepQueryDir = con->prepareStatement("SELECT id,date FROM "DIRECTORIES_TABLE" WHERE parent=? AND name=?");
    prepInsertDir = con->prepareStatement("INSERT INTO "DIRECTORIES_TABLE" (name,parent,date) VALUES (?, ?, FROM_UNIXTIME(?))");
    prepUpdateDirSize = con->prepareStatement("UPDATE "DIRECTORIES_TABLE" SET size=? WHERE id=?"); //directories won't have their date changed on size change
    prepUpdateDirDate = con->prepareStatement("UPDATE "DIRECTORIES_TABLE" SET date=FROM_UNIXTIME(?) WHERE id=?");

    prepLastInsertID = con->prepareStatement("SELECT LAST_INSERT_ID()");

    //Advance to the specified fakepath
    unsigned int id = 0;
    if( !fakepath.empty() ) {
      debug("processing fakepath "+fakepath,3);
      while( !fakepath.empty() ) {
        string temppath = fakepath.substr(0,fakepath.find('/'));
        fakepath.erase(0,temppath.length()+1);
        if( temppath.empty() )
          continue;
        id = addDirectory(id,temppath,time(0));
      }
    }

    //Save starting time
    debug("starting parser",3);
    time_t start = time(0);

    parseDirectory(id,basedir);

    //Get time now, calculate and output time
    time_t end = time(0);
    end -= start;
    unsigned int temp = end;
    if( verbosity ) {
      cout << "Parsed " << filecount << " files in ";
      for(temp = 0; end > 3600; temp++)
        end-=3600;
      if( temp )
        cout << temp << "h";
      for(temp = 0; end > 60; temp++)
        end-=60;
      if( temp )
        cout << temp << "m";
      cout << end << "s" << endl;
    }
  }

  delete stmt;
  con->close();
  delete con;

  return 0;
}