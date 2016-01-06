#include <cstring>
#include <iostream>
#include <string>
#include <csignal>

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <mysql_connection.h>

#include "worker.h"
#include "logger.h"
#include "hasher.h"
#include "options.h"

using namespace std;

static worker* w = 0;
static sql::Connection* con = 0;

void initFakepath(worker* w, uint32_t& fakepathId, const string& fakepath) {
  if( !fakepath.empty() ) {
    LOG(logInfo) << "Using fakepath \"" << fakepath << '\"';
    fakepathId = w->descendPath(fakepath);
    LOG(logDebug) << "got fakepathId " << fakepathId;
  }
}

void cleanup() {
  if (w)
    delete w;
  if (con) {
    con->close();
    delete con;
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

int main(int argc, char* argv[]) {
  switch (OPTS.parse(argc, argv)) {
    case 1 : return 0; // immediate quit (help, version)
    case 2 : return 1; // parse error
  }

  string basedir = OPT_STR("basedir");
  string fakepath = OPT_STR("fakepath");

  try {
    LOG(logInfo) << "Connecting to SQL server";
    sql::ConnectOptionsMap options;
    //options["hostName"] = OPTS["host"].as<string>;
    options["hostName"] = OPT_STR("host");
    options["userName"] = OPT_STR("user");
    options["password"] = OPT_STR("password");
    options["schema"]   = OPT_STR("database");
    //we handle reconnecting on our own to ensure that prepared statements are re-prepared after lossing connection
    //however this option is useful for debugging if an unprotected operation such as statement prepare is executed
    options["OPT_RECONNECT"] = true;
    con = get_driver_instance()->connect(options);
  } catch( exception& e ) {
    LOG(logError) << "Failed to connect: " << e.what();
    exit(1);
  }

  LOG(logDebug) << "setting up worker";
  w = new worker(con);
  w->setTables(OPT_STR("dir-table"),OPT_STR("file-table"));

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
  } catch( sql::SQLException& e ) {
    LOG(logError) << "SQL Exception: " << e.what();
    exit(1);
  } catch( exception& e ) {
    LOG(logError) << "Unhandled Exception: " << e.what();
    exit(1);
  }

  cleanup();

  return 0;
}
