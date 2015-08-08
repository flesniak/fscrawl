#include <cstring>
#include <iostream>
#include <string>

#include <cppconn/driver.h>
#include <mysql_connection.h>

#include "worker.h"
#include "logger.h"
#include "hasher.h"

#define VERSION "2.4"

using namespace std;

void usage() {
  LOG(logInfo) << "Usage: fscrawl [OPTIONS] <BASEDIR|-V|--clear|--purge>";
  LOG(logInfo) << "  -d, --database\tDatabase to use (default: \"fscrawl\")";
  LOG(logInfo) << "  -f, --fakepath\tInstead of having BASEDIR as absolute root directory,";
  LOG(logInfo) << "                \tparse all files as if they were unter this fakepath";
  LOG(logInfo) << "  -h, --help\tDisplay this help and exit";
  LOG(logInfo) << "  -l, --logfile\tLog to file instead of stderr";
  LOG(logInfo) << "  -m, --host\tConnect to host (default: \"localhost\"";
  LOG(logInfo) << "  -p, --password\tSpecify database user password (default: none)";
  LOG(logInfo) << "  -q, --quiet\tOnly display severe errors. Useful for cron-jobbing";
  LOG(logInfo) << "  -u, --user\tSpecify database user (default: \"root\")";
  LOG(logInfo) << "  -v, --verbose\tIncrease log level (0x=info,1x=detailed,2x=debug)";
  LOG(logInfo) << "  -w, --watch\tWatch the given BASEDIR after refreshing (program will block)";
  LOG(logInfo) << "  -S, --sha1\tCalculate the sha1 hash of every file";
  LOG(logInfo) << "  -M, --md5\tCalculate the md5 hash of every file";
  LOG(logInfo) << "  -T, --tth\tCalculate the tth hash of every file";
  LOG(logInfo) << "  -c, --check\tCheck the hash of every file (needs -T/M/S)";
  LOG(logInfo) << "             \tNo crawling will be done if checking is enabled";
  LOG(logInfo) << "  -F, --force-hashing\tForce recalculation of every hash (use when changing algorithm)";
  LOG(logInfo) << "  --file-table\tTable to use for files (default: \"fscrawl_files\")";
  LOG(logInfo) << "  --dir-table\tTable to use for directories (default: \"fscrawl_directories\")";
  LOG(logInfo) << "The following options may be called without BASEDIR, the database won't be changed:";
  LOG(logInfo) << "  -V, --verify\tVerify the tree structure - takes some time";
  LOG(logInfo) << "  --clear\tDelete the tree for this fakepath, others will be kept";
  LOG(logInfo) << "  --purge\tDelete all data from both tables completely";
  LOG(logInfo) << "fscrawl "VERSION;
  exit(1);
}

int main(int argc, char* argv[]) {
  bool verifyTree = false;
  bool hashCheck = false;
  bool forceHashing = false;
  bool watch = false;
  Hasher::hashType_t hashType = Hasher::noHash;
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

  //TODO: maybe switch to gnu getopt in future?
  for(int i = 1; i < argc; i++) {
    if( strcmp(argv[i],"-h") == 0 || strcmp(argv[i],"--help") == 0 ) {
      usage();
    }
    if( strcmp(argv[i],"-v") == 0 || strcmp(argv[i],"--verbose") == 0 ) {
      Logger::logLevel() = (logLevel_t)( (uint32_t)Logger::logLevel() + 1 );
      continue;
    }
    if( strcmp(argv[i],"-vv") == 0 ) {
      Logger::logLevel() = (logLevel_t)( (uint32_t)Logger::logLevel() + 2 );
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
      }
      i++;
      mysql_user = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-p") == 0 || strcmp(argv[i],"--password") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected password";
        usage();
      }
      i++;
      mysql_password = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-m") == 0 || strcmp(argv[i],"--host") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected host";
        usage();
      }
      i++;
      mysql_host = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-d") == 0 || strcmp(argv[i],"--database") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected database";
        usage();
      }
      i++;
      mysql_database = argv[i];
      continue;
    }
    if( strcmp(argv[i],"--file-table") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected file table";
        usage();
      }
      i++;
      fileTable = argv[i];
      continue;
    }
    if( strcmp(argv[i],"--dir-table") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected directory table";
        usage();
      }
      i++;
      directoryTable = argv[i];
      continue;
    }
    if( strcmp(argv[i],"--clear") == 0 ) {
      clear = clearFakepath;
      continue;
    }
    if( strcmp(argv[i],"--purge") == 0 ) {
      clear = clearAll;
      continue;
    }
    if( strcmp(argv[i],"-f") == 0 || strcmp(argv[i],"--fakepath") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected fakepath";
        usage();
      }
      i++;
      fakepath = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-l") == 0 || strcmp(argv[i],"--logfile") == 0 ) {
      if( i+1 >= argc ) {
        LOG(logError) << "expected logfile";
        usage();
      }
      i++;
      logfile = argv[i];
      continue;
    }
    if( strcmp(argv[i],"-V") == 0 || strcmp(argv[i],"--verify") == 0 ) {
      verifyTree = true;
      continue;
    }
    if( strcmp(argv[i],"-c") == 0 || strcmp(argv[i],"--check") == 0 ) {
      hashCheck = true;
      continue;
    }
    if( strcmp(argv[i],"-w") == 0 || strcmp(argv[i],"--watch") == 0 ) {
      watch = true;
      continue;
    }
    if( strcmp(argv[i],"-T") == 0 || strcmp(argv[i],"--tth") == 0 ) {
      if( hashType != Hasher::noHash ) {
        LOG(logError) << "Multiple hash algorithms specified";
        usage();
      }
      hashType = Hasher::tth;
      continue;
    }
    if( strcmp(argv[i],"-M") == 0 || strcmp(argv[i],"--md5") == 0 ) {
      if( hashType != Hasher::noHash ) {
        LOG(logError) << "Multiple hash algorithms specified";
        usage();
      }
      hashType = Hasher::md5;
      continue;
    }
    if( strcmp(argv[i],"-S") == 0 || strcmp(argv[i],"--sha1") == 0 ) {
      if( hashType != Hasher::noHash ) {
        LOG(logError) << "Multiple hash algorithms specified";
        usage();
      }
      hashType = Hasher::sha1;
      continue;
    }
    if( strcmp(argv[i],"-F") == 0 || strcmp(argv[i],"--force-hashing") == 0 ) {
      forceHashing = true;
      continue;
    }
    if( strncmp(argv[i],"-",1) == 0 ) {
      LOG(logError) << "Unknown argument " << argv[i];
      usage();
    }
    if( basedir.empty() )
      basedir = argv[i];
    else {
      LOG(logError) << "Invalid additional argument " << argv[i];
      usage();
    }
  }

  if( !logfile.empty() ) {
    LoggerFacilityFile* lff = new LoggerFacilityFile;
    if( !lff->openLogFile(logfile, false) ) {
      LOG(logError) << "failed to open logfile \"" << logfile << '\"';
      usage();
    }
    delete Logger::facility();
    Logger::facility() = lff;
  }

  LOG(logInfo) << "fscrawl "VERSION << " started";

  //check if our given commands can operate without basedir
  if( basedir.empty() ) {
    if( clear == clearNothing && !verifyTree ) { //if no basedir is given but clear is set, only clear the db and exit
      LOG(logError) << "No basedir given";
      usage();
    }
  }
  else
    if( basedir.at(basedir.size()-1) == '/' ) //don't try that on an empty basedir string
      basedir.erase(basedir.size()-1);

  //check parameters that need a hash algorithm selected
  if( (hashCheck || forceHashing) && hashType == Hasher::noHash ) {
    LOG(logError) << "No hashing algorithm selected";
    usage();
  }

  if( (watch || forceHashing) && hashCheck ) {
    LOG(logError) << "No crawling-dependant jobs (watching, (forced) hashing) can be combined with hash checking.";
    usage();
  }

  sql::Driver *driver = 0;
  sql::Connection *con = 0;
  try {
    LOG(logInfo) << "Connecting to SQL server";
    driver = get_driver_instance();
    con = driver->connect(mysql_host,mysql_user,mysql_password);
    con->setSchema(mysql_database);
  } catch( exception& e ) {
    LOG(logError) << "Failed to connect: " << e.what();
    exit(1);
  }

  LOG(logDebug) << "setting up worker";
  worker* w = new worker(con);
  w->setTables(directoryTable,fileTable);

  if( hashType != Hasher::noHash )
    w->setHasher( new Hasher(hashType) );

  if( clear == clearAll ) {
    LOG(logWarning) << "Clearing database";
    w->clearDatabase();
  }

  if( verifyTree ) {
    LOG(logInfo) << "Verifying tree";
    w->verifyTree();
    LOG(logInfo) << "Tree verified";
  }

  //descend to given fakepath
  uint32_t fakepathId = 0; //if no fakepath is used, 0 is the root parent directory id
  if( !fakepath.empty() ) {
    LOG(logInfo) << "Using fakepath \"" << fakepath << '\"';
    fakepathId = w->descendPath(fakepath);
    LOG(logDebug) << "got fakepathId " << fakepathId;
    if( clear == clearFakepath ) {
      LOG(logWarning) << "Deleting everything on fakepath \"" << fakepath << '\"';
      w->deleteDirectory(fakepathId);
      if( !basedir.empty() )
        fakepathId = w->descendPath(fakepath);
    }
  }

  if( !basedir.empty() ) {
    //Save starting time
    time_t start = time(0);

    if( hashCheck ) { //do a hash check, skip crawling!
      LOG(logInfo) << "Checking hashes of files in directory \"" << basedir << '\"';
      w->hashCheck(basedir, fakepathId);
    } else { //normal crawling
      LOG(logInfo) << "Parsing directory \"" << basedir << '\"';
      w->parseDirectory(basedir, fakepathId);
    }

    //Get time now, calculate and output duration
    double duration = difftime(time(0),start);
    unsigned int hours, minutes;
    for(hours = 0; duration > 3600; hours++)
      duration -= 3600;
    for(minutes = 0; duration > 60; minutes++)
      duration -= 60;
    LOG(logInfo) << "Processed "
                 << w->getStatistics().files << " files and "
                 << w->getStatistics().directories << " directories in "
                 << hours << "h"
                 << minutes << "m"
                 << duration << "s";

    if( watch ) { //hashCheck && watch is already filtered out above
      LOG(logInfo) << "Running watch on " << fakepath;
      w->watch(basedir,fakepathId);
    }
  }

  delete w;
  con->close();
  delete con;

  return 0;
}
