#include <cstring>
#include <iostream>
#include <string>
#include <csignal>
#include <dirent.h>

#include <mysql.h>

#include "worker.h"
#include "logger.h"
#include "hasher.h"
#include "options.h"
#include "sqlexception.h"

using namespace std;

static worker* w = 0;
static MYSQL* con = 0;

void initFakepath(worker* w, uint32_t& fakepathId, const string& fakepath) {
  if( !fakepath.empty() ) {
    LOG(logInfo) << "Using fakepath \"" << fakepath << '\"';
    fakepathId = w->descendPath(fakepath);
    LOG(logDebug) << "got fakepathId " << fakepathId;
  }
}

void cleanup() {
  if (w) {
    delete w;
    w = 0;
  }
  if (con) {
    mysql_close(con);
    con = 0;
  }
}

void signalHandler(int signum) {
  static int interruptsReceived = 0;
  switch (interruptsReceived) {
    case 0 :
      LOG(logDebug) << "Signal " << signum << " received. Sending abort to worker.";
      w->abort();
      break;
    default :
      LOG(logWarning) << "Second interrupt received, terminating";
      cleanup();
      exit(1);
      break;
  }
  interruptsReceived++;
}

bool directoryEmpty(const string& path) {
  DIR* dir = opendir(path.c_str());
  if( !dir ) {
    LOG(logError) << "failed to read directory " << path << ": " << worker::errnoString();
    return false;
  }

  int n = 0;
  while( readdir(dir) ) { // readdir returns NULL when end of directory is reached
    if( ++n > 2 ) // more files than . and .. -> directory is not empty
      break;
  }
  closedir(dir);

  if (n <= 2)
    return true;
  else
    return false;
}

int main(int argc, char* argv[]) {
  switch (OPTS.parse(argc, argv)) {
    case 1 : return 0; // immediate quit (help, version)
    case 2 : return 1; // parse error
  }

  string basedir = OPT_STR("basedir");
  string fakepath = OPT_STR("fakepath");

  try {
    LOG(logInfo) << "Connecting to SQL server";
    con = mysql_init(0);

    bool reconnect = 1;
    mysql_optionsv(con, MYSQL_OPT_RECONNECT, &reconnect);
    mysql_optionsv(con, MYSQL_OPT_COMPRESS, 0);

    MYSQL* res = mysql_real_connect(con,
      OPT_STR("host").c_str(),
      OPT_STR("user").c_str(),
      OPT_STR("password").c_str(),
      OPT_STR("database").c_str(),
      0, /* port number, 0 for default */
      NULL, /* socket file or named pipe name */
      CLIENT_FOUND_ROWS | CLIENT_MULTI_STATEMENTS);
    if (!res)
      throw runtime_error("mysql_real_connect failed: "+string(mysql_error(con)));
  } catch( exception& e ) {
    LOG(logError) << "Failed to connect: " << e.what();
    exit(1);
  }

  LOG(logDebug) << "setting up worker";
  w = new worker(con);
  w->setTables(OPT_STR("dir-table"),OPT_STR("file-table"));
  w->setDryRun(options::getInstance().count("dry-run"));

  signal(SIGINT, signalHandler);
  signal(SIGTERM, signalHandler);

  Hasher::hashType_t hashType = OPTS.hashType();
  if (hashType != Hasher::noHash)
    w->setHasher(new Hasher(hashType));

  uint32_t fakepathId = 0; //if no fakepath is used, 0 is the root parent directory id

  //Save starting time
  time_t start = time(0);

  try {
    switch (OPTS.getOperation()) {
      case options::opCrawl :
        if( directoryEmpty(basedir) ) {
          if (!OPTS.allowEmpty()) {
            LOG(logError) << "Basedir " << basedir << " is empty. Use --allow-empty if intended or --clear to delete all files";
            exit(1);
          } else {
            LOG(logWarning) << "Basedir " << basedir << " is empty, will remove all files.";
          }
        }
        initFakepath(w, fakepathId, fakepath);
        LOG(logInfo) << "Parsing directory \"" << basedir << '\"';
        w->parseDirectory(basedir, fakepathId);
        break;
      case options::opCheck :
        initFakepath(w, fakepathId, fakepath);
        LOG(logInfo) << "Checking hashes of files in directory \"" << basedir << '\"';
        w->hashCheck(basedir, fakepathId);
        break;
      case options::opVerify :
        LOG(logInfo) << "Verifying tree";
        w->verifyTree();
        LOG(logInfo) << "Tree verified";
        break;
      case options::opPrint :
        initFakepath(w, fakepathId, fakepath);
        LOG(logInfo) << "Printing tree";
        w->printTree(fakepathId);
        LOG(logInfo) << "Tree printed";
        break;
      case options::opClear :
        initFakepath(w, fakepathId, fakepath);
        LOG(logWarning) << "Deleting everything on fakepath \"" << fakepath << '\"';
        w->deleteDirectory(fakepathId);
        break;
      case options::opPurge :
        LOG(logWarning) << "Clearing database";
        w->clearDatabase();
        break;
      default :
        LOG(logError) << "Unhandled operation mode, BUG?!";
        return 1;
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

    if (OPTS.getOperation() == options::opCrawl && OPTS.watch()) {
      LOG(logInfo) << "Entering watch mode on " << basedir;
      w->watch(basedir, fakepathId);
      LOG(logInfo) << "Finished watching";
    }
  } catch( SQLException& e ) {
    LOG(logError) << "SQL Exception: " << e.what();
    exit(1);
  } catch( exception& e ) {
    LOG(logError) << "Unhandled Exception: " << e.what();
    exit(1);
  }

  cleanup();

  return 0;
}
