/* compile with:
 * g++ -I /usr/include/mysql-connector/ -lmysqlcppconn -lstdc++ -O2 -o fscrawl fscrawl.cpp */

#include <cstring>
#include <iostream>
#include <string>

#include <cppconn/driver.h>
#include <mysql_connection.h>

#include "worker.h"

#define VERSION "1.50alpha"

using namespace std;

worker::logLevel logLevel;

void usage() {
  cout << "Usage: fscrawl <MODE> [OPTIONS] <basedir|-c>" << endl;
  cout << "  -h, --help\tDisplay this help and exit" << endl;
  cout << "  -u, --user\tSpecify database user (default: \"root\")" << endl;
  cout << "  -p, --password\tSpecify database user password (default: none)" << endl;
  cout << "  -m, --host\tConnect to host (default: \"localhost\"" << endl;
  cout << "  -d, --database\tDatabase to use (default: \"fscrawl\")" << endl;
  cout << "  -f, --fakepath\tInstead of having basedir as absolute root directory, parse all files as if they were unter this fakepath" << endl;
  cout << "  -v, --verbose\tIncrease log level (0x=status,1x=detailed,2x=debug)" << endl;
  cout << "  -q, --quiet\tOnly display severe errors. Useful for cron-jobbing" << endl;
  cout << endl << "The following options may be called without a basedir, then no scanning will be done:" << endl;
  cout << "  -c, --clear\tDelete the tree for this fakepath, others will be kept" << endl;
  cout << "  -C, --clearall\tClear the database - ALL DATA WILL BE DELETED" << endl;
  cout << "  -V, --verify\tVerify the tree structure - takes some time" << endl;
  cout << endl << "fscrawl version "VERSION << endl;
}

inline void log(const string& message, worker::logLevel level) {
  if( logLevel >= level )
    cout << "V" << (uint32_t)level << ": " << message << endl;
}

int main(int argc, char* argv[]) {
  logLevel = worker::status; //default, display stats after crawling
  bool verify = false;
  unsigned char clear = 0;
  string basedir;
  string fakepath;
  string mysql_user("root");
  string mysql_password;
  string mysql_host("localhost");
  string mysql_database("fscrawl");

  log("parsing command line arguments",worker::debug);
  for(int i = 1; i < argc; i++) {
    if( strcmp(argv[i],"-h") == 0 || strcmp(argv[i],"--help") == 0 ) {
      usage();
      exit(1);
    }
    if( strcmp(argv[i],"-v") == 0 || strcmp(argv[i],"--verbose") == 0) {
      logLevel = (worker::logLevel)((uint32_t)logLevel+1);
      continue;
    }
    if( strcmp(argv[i],"-vv") == 0 ) {
      logLevel = (worker::logLevel)((uint32_t)logLevel+2);
      continue;
    }
    if( strcmp(argv[i],"-vvv") == 0 ) {
      logLevel = (worker::logLevel)((uint32_t)logLevel+3);
      continue;
    }
    if( strcmp(argv[i],"-q") == 0 || strcmp(argv[i],"--quiet") == 0 ) {
      logLevel = worker::quiet;
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

  log("initializing sql driver",worker::debug);
  sql::Driver *driver = get_driver_instance();
  sql::Connection *con = driver->connect(mysql_host,mysql_user,mysql_password);
  con->setSchema(mysql_database);

  worker* w = new worker(con);
  w->setLogLevel(logLevel);

  if( clear == 2 ) {
    w->clearDatabase();
  }
  if( verify ) {
    w->verifyTree();
  }
  if( !basedir.empty() ) {
    //ascend to given fakepath
    w->ascendFakepath(fakepath);

    //Save starting time
    log("starting parser",worker::debug);
    time_t start = time(0);

    w->parseDirectory(basedir);

    //Get time now, calculate and output duration
    double duration = difftime(start,time(0));
    if( logLevel > worker::quiet ) {
      unsigned int temp;
      cout << "Parsed " << w->getStatistics().files << " files and " << w->getStatistics().directories << " directories in ";

      //Count hours
      for(temp = 0; duration > 3600; temp++)
        duration -= 3600;
      if( temp )
        cout << temp << "h";

      //Count minutes
      for(temp = 0; duration > 60; temp++)
        duration -= 60;
      if( duration )
        cout << temp << "m";

      //Print seconds
      cout << duration << "s" << endl;
    }
  }

  delete w;
  con->close();
  delete con;

  return 0;
}