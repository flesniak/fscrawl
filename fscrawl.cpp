#include <cstring>
#include <iostream>
#include <string>

#include <cppconn/driver.h>
#include <mysql_connection.h>

#include "worker.h"
#include "logger.h"

#define VERSION "2.0"

using namespace std;

void usage() {
  LOG(logInfo) << "Usage: fscrawl [OPTIONS] <basedir|-c|-C|-V>";
  LOG(logInfo) << "  -d, --database\tDatabase to use (default: \"fscrawl\")";
  LOG(logInfo) << "  -f, --fakepath\tInstead of having basedir as absolute root directory, parse all files as if they were unter this fakepath";
  LOG(logInfo) << "  -h, --help\tDisplay this help and exit";
  LOG(logInfo) << "  -l, --logfile\tLog to file instead of stderr";
  LOG(logInfo) << "  -m, --host\tConnect to host (default: \"localhost\"";
  LOG(logInfo) << "  -p, --password\tSpecify database user password (default: none)";
  LOG(logInfo) << "  -q, --quiet\tOnly display severe errors. Useful for cron-jobbing";
  LOG(logInfo) << "  -u, --user\tSpecify database user (default: \"root\")";
  LOG(logInfo) << "  -v, --verbose\tIncrease log level (0x=info,1x=detailed,2x=debug)";
  LOG(logInfo) << "  -w, --watch\tWatch the given basedir after refreshing (program will block)";
  LOG(logInfo) << "  --file-table\tTable to use for files (default: \"fscrawl_files\")";
  LOG(logInfo) << "  --dir-table\tTable to use for directories (default: \"fscrawl_directories\")";
  LOG(logInfo) << "The following options may be called without a basedir, then no scanning will be done:";
  LOG(logInfo) << "  -c, --clear\tDelete the tree for this fakepath, others will be kept";
  LOG(logInfo) << "  -C, --clearall\tClear both tables completely";
  LOG(logInfo) << "  -V, --verify\tVerify the tree structure - takes some time";
  LOG(logInfo) << "fscrawl "VERSION;
}

int main(int argc, char* argv[]) {
  bool verify = false;
  bool watch = false;
  enum { clearNothing, clearFakepath, clearAll } clear = clearNothing;
  string basedir;
  string fakepath;
  string directoryTable;
  string fileTable;
  string logfile;
  string mysql_user("root");
  string mysql_password;
  string mysql_host("localhost");
  string mysql_database("fscrawl");
  Logger::logLevel() = logInfo;
  Logger::facility() = new LoggerFacilityConsole;

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
        LOG(logError) << "expected user";
        usage();
        exit(1);
      }
      i++;
      mysql_user = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-p") == 0 || strcmp(argv[i],"--password") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected password";
        usage();
        exit(1);
      }
      i++;
      mysql_password = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-m") == 0 || strcmp(argv[i],"--host") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected host";
        usage();
        exit(1);
      }
      i++;
      mysql_host = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-d") == 0 || strcmp(argv[i],"--database") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected database";
        usage();
        exit(1);
      }
      i++;
      mysql_database = argv[i];
      continue;
    }
    if( strcmp(argv[i],"--file-table") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected file table";
        usage();
        exit(1);
      }
      i++;
      fileTable = argv[i];
      continue;
    }
    if( strcmp(argv[i],"--dir-table") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected directory table";
        usage();
        exit(1);
      }
      i++;
      directoryTable = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-c") == 0 || strcmp(argv[i],"--clear") == 0 ) {
      clear = clearFakepath;
      continue;
    }
    if( strcmp(argv[i],"-C") == 0 || strcmp(argv[i],"--clearall") == 0 ) {
      clear = clearAll;
      continue;
    }
    if( strcmp(argv[i],"-f") == 0 || strcmp(argv[i],"--fakepath") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected fakepath";
        usage();
        exit(1);
      }
      i++;
      fakepath = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-l") == 0 || strcmp(argv[i],"--logfile") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected logfile";
        usage();
        exit(1);
      }
      i++;
      logfile = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-V") == 0 || strcmp(argv[i],"--verify") == 0 ) {
      verify = true;
      continue;
    }
    if( strcmp(argv[i],"-w") == 0 || strcmp(argv[i],"--watch") == 0 ) {
      watch = true;
      continue;
    }
    if( strncmp(argv[i],"-",1) == 0 ) {
      LOG(logError) << "unknown argument " << argv[i];
      usage();
      exit(1);
    }
    if( basedir.empty() )
      basedir = argv[i];
    else {
      LOG(logError) << "invalid additional argument " << argv[i];
      usage();
      exit(1);
    }
  }

  if( !logfile.empty() ) {
    LoggerFacilityFile* lff = new LoggerFacilityFile;
    if( !lff->openLogFile(logfile, false) ) {
      LOG(logError) << "failed to open logfile \"" << logfile << '\"';
      usage();
      exit(1);
    }
    delete Logger::facility();
    Logger::facility() = lff;
  }

  LOG(logInfo) << "fscrawl "VERSION << " started";

  if( basedir.empty() ) {
    if( clear == clearNothing && !verify ) { //if no basedir is given but clear is set, only clear the db and exit
      LOG(logError) << "no basedir given";
      usage();
      exit(1);
    }
  }
  else
    if( basedir.at(basedir.size()-1) == '/' ) //don't try that on an empty basedir string
      basedir.erase(basedir.size()-1);

  LOG(logInfo) << "Connecting to SQL server";
  sql::Driver *driver = get_driver_instance();
  sql::Connection *con = driver->connect(mysql_host,mysql_user,mysql_password);
  con->setSchema(mysql_database);

  LOG(logDebug) << "setting up worker";
  worker* w = new worker(con);
  w->setTables(directoryTable,fileTable);

  if( clear == clearAll ) {
    LOG(logWarning) << "Clearing database";
    w->clearDatabase();
  }

  if( verify ) {
    LOG(logInfo) << "Verifying tree";
    w->verifyTree();
    LOG(logInfo) << "Tree verified";
  }

  //ascend to given fakepath
  uint32_t fakepathId = 0;
  if( !fakepath.empty() ) {
    LOG(logInfo) << "Using fakepath \"" << fakepath << '\"';
    fakepathId = w->ascendPath(fakepath);
    LOG(logDebug) << "got fakepathId " << fakepathId;
    if( clear == clearFakepath ) {
      LOG(logWarning) << "Deleting everything on fakepath \"" << fakepath << '\"';
      w->deleteDirectory(fakepathId);
      if( !basedir.empty() )
        fakepathId = w->ascendPath(fakepath);
    }
  }

  if( !basedir.empty() ) {
    //Save starting time
    LOG(logInfo) << "Parsing directory \"" << basedir << '\"';
    time_t start = time(0);

    w->parseDirectory(basedir, fakepathId);

    //Get time now, calculate and output duration
    double duration = difftime(time(0),start);
    unsigned int hours, minutes;
    for(hours = 0; duration > 3600; hours++) //Count hours
      duration -= 3600;
    for(minutes = 0; duration > 60; minutes++) //Count minutes
      duration -= 60;
    LOG(logInfo) << "Parsed " << w->getStatistics().files << " files and " << w->getStatistics().directories << " directories in " << hours << "h" << minutes << "m" << duration << "s";

    if( watch ) {
      LOG(logInfo) << "Running watch on " << fakepath;
      w->watch(basedir,fakepathId);
    }
  }

  delete w;
  con->close();
  delete con;

  return 0;
}