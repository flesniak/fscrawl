#include <cstring>
#include <iostream>
#include <string>

#include <cppconn/driver.h>
#include <mysql_connection.h>

#include "worker.h"
#include "logger.h"

#define VERSION "1.99alpha"

using namespace std;

void usage() {
  cout << "Usage: fscrawl <MODE> [OPTIONS] <basedir|-c>";
  cout << "  -h, --help\tDisplay this help and exit";
  cout << "  -u, --user\tSpecify database user (default: \"root\")";
  cout << "  -p, --password\tSpecify database user password (default: none)";
  cout << "  -m, --host\tConnect to host (default: \"localhost\"";
  cout << "  -d, --database\tDatabase to use (default: \"fscrawl\")";
  cout << "  -f, --fakepath\tInstead of having basedir as absolute root directory, parse all files as if they were unter this fakepath";
  cout << "  --file-table\tTable to use for files (default: \"fscrawl_files\")";
  cout << "  --dir-table\tTable to use for directories (default: \"fscrawl_directories\")";
  cout << "  -v, --verbose\tIncrease log level (0x=info,1x=detailed,2x=debug)";
  cout << "  -q, --quiet\tOnly display severe errors. Useful for cron-jobbing";
  cout << "The following options may be called without a basedir, then no scanning will be done:";
  cout << "  -c, --clear\tDelete the tree for this fakepath, others will be kept";
  cout << "  -C, --clearall\tClear the database - ALL DATA WILL BE DELETED";
  cout << "  -V, --verify\tVerify the tree structure - takes some time";
  cout << "fscrawl version "VERSION;
}

int main(int argc, char* argv[]) {
  bool verify = false;
  unsigned char clear = 0;
  string basedir;
  string fakepath;
  string directoryTable;
  string fileTable;
  string mysql_user("root");
  string mysql_password;
  string mysql_host("localhost");
  string mysql_database("fscrawl");
  Logger::logLevel() = logInfo;

  for(int i = 1; i < argc; i++) {
    if( strcmp(argv[i],"-h") == 0 || strcmp(argv[i],"--help") == 0 ) {
      usage();
      exit(1);
    }
    if( strcmp(argv[i],"-v") == 0 || strcmp(argv[i],"--verbose") == 0 ) {
      Logger::logLevel() = (logLevel_t)( (uint32_t)Logger::logLevel() + 1 );
      continue;
    }
    if( strcmp(argv[i],"-vv") == 0 ) {
      Logger::logLevel() = (logLevel_t)( (uint32_t)Logger::logLevel() + 2 );
      continue;
    }
    if( strcmp(argv[i],"-vvv") == 0 ) {
      Logger::logLevel() = (logLevel_t)( (uint32_t)Logger::logLevel() + 3 );
      continue;
    }
    if( strcmp(argv[i],"-q") == 0 || strcmp(argv[i],"--quiet") == 0 ) {
      Logger::logLevel() = logError;
      continue;
    }
    if( strcmp(argv[i],"-u") == 0 || strcmp(argv[i],"--user") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "ERROR: expected user";
        usage();
        exit(1);
      }
      i++;
      mysql_user = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-p") == 0 || strcmp(argv[i],"--password") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "ERROR: expected password";
        usage();
        exit(1);
      }
      i++;
      mysql_password = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-m") == 0 || strcmp(argv[i],"--host") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "ERROR: expected host";
        usage();
        exit(1);
      }
      i++;
      mysql_host = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-d") == 0 || strcmp(argv[i],"--database") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "ERROR: expected database";
        usage();
        exit(1);
      }
      i++;
      mysql_database = argv[i];
      continue;
    }
    if( strcmp(argv[i],"--file-table") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "ERROR: expected file table";
        usage();
        exit(1);
      }
      i++;
      fileTable = argv[i];
      continue;
    }
    if( strcmp(argv[i],"--dir-table") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "ERROR: expected directory table";
        usage();
        exit(1);
      }
      i++;
      directoryTable = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-C") == 0 || strcmp(argv[i],"--clearall") == 0 ) {
      clear = true;
      continue;
    }
    if( strcmp(argv[i],"-f") == 0 || strcmp(argv[i],"--fakepath") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "ERROR: expected fakepath";
        usage();
        exit(1);
      }
      i++;
      fakepath = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-V") == 0 || strcmp(argv[i],"--verify") == 0 ) {
      verify = true;
      continue;
    }
    if( strncmp(argv[i],"-",1) == 0 ) {
      LOG(logError) << "ERROR: unknown argument " << argv[i];
      usage();
      exit(1);
    }
    if( basedir.empty() )
      basedir = argv[i];
    else {
      LOG(logError) << "ERROR: invalid additional argument " << argv[i];
      usage();
      exit(1);
    }
  }

  if( basedir.empty() ) {
    if( !clear && !verify ) { //if no basedir is given but clear is set, only clear the db and exit
      cout << "ERROR: no basedir given";
      usage();
      exit(1);
    }
  }
  else
    if( basedir.at(basedir.size()-1) == '/' ) //don't try that on an empty basedir string
      basedir.erase(basedir.size()-1);

  Logger::facility() = new LoggerFacilityConsole;

  LOG(logDebug) << "initializing sql driver";
  sql::Driver *driver = get_driver_instance();
  sql::Connection *con = driver->connect(mysql_host,mysql_user,mysql_password);
  con->setSchema(mysql_database);

  worker* w = new worker(con);
  w->setTables(directoryTable,fileTable);

  if( clear == 2 ) {
    w->clearDatabase();
  }
  if( verify ) {
    w->verifyTree();
  }
  if( !basedir.empty() ) {
    //ascend to given fakepath
    uint32_t fakepathId = w->ascendPath(fakepath);
    cout << "fakepathId " << fakepathId;
    /*//Save starting time
    LOG(logDebug) << "starting parser";
    time_t start = time(0);

    w->parseDirectory(basedir, fakepathId);

    //Get time now, calculate and output duration
    double duration = difftime(start,time(0));
    if( logLevel > worker::quiet ) {
      unsigned int hours, minutes;

      //Count hours
      for(hours = 0; duration > 3600; hours++)
        duration -= 3600;

      //Count minutes
      for(minutes = 0; duration > 60; minutes++)
        duration -= 60;

      LOG(logDebug) << "Parsed " << w->getStatistics().files << " files and " << w->getStatistics().directories << " directories in " << hours << "h" << minutes << "m" << duration << "s";
    }*/
  }

  delete w;
  con->close();
  delete con;

  return 0;
}