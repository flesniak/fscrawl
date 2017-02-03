#include "options.h"
#include "logger.h"
#include "version.h"

#include <string>

using namespace boost::program_options;
using namespace std;

options::options()
  : p_opts_mode("Operation modes"),
    p_opts_required("Required parameters"),
    p_opts_optional("Optional parameters"),
    p_opts_all("Allowed arguments"),
    p_operation(opNone) {
  // Attention: order of p_opts_mode has to correspond to operation_t
  p_opts_mode.add_options()
    ("crawl", "Crawl for new and changed files (default)")
    ("check,c", "Check the hash of every file (requires -T/M/S)")
    ("verify,v", "Verify the tree structure - takes some time")
    ("print,P", "Print the tree structure to standard output")
    ("clear", "Delete the tree for this fakepath, others will be kept")
    ("purge", "Delete all data from both tables completely")
    ("help,h", "Display this help and exit")
    ("version,V", "Print the version and exit")
  ;

  p_opts_required.add_options()
    ("basedir,b", value<string>(&p_basedir)->default_value(""), "Root directory to work on (for crawl/check)")
  ;

  p_opts_optional.add_options()
    ("loglevel,l", value<int>()->default_value(logInfo), "Set log level (0-4) (error, warning, info (default), detailed, debug)")
    ("logfile,L", value<string>(), "Log to file instead of stderr")
    ("fakepath,f", value<string>()->default_value(""), "Instead of having basedir as absolute root directory, parse all files as if they were unter this fakepath")
    ("watch,w", "Watch the given BASEDIR after crawling (program will block)")
    ("database,d", value<string>()->default_value("fscrawl"), "Database to use")
    ("host,m", value<string>()->default_value("localhost"), "Database host to connect to")
    ("user,u", value<string>()->default_value("root"), "Specify database user")
    ("password,p", value<string>()->default_value(""), "Specify database user password")
    ("sha1,S", "Calculate the SHA1 hash of every file")
    ("md5,M", "Calculate the MD5 hash of every file")
    ("tth,T", "Calculate the TTH hash of every file")
    ("force-hashing,F", "Force recalculation of every hash (use when changing algorithm)")
    ("file-table", value<string>()->default_value("fscrawl_files"), "Table to use for files")
    ("dir-table", value<string>()->default_value("fscrawl_directories"), "Table to use for directories")
    ("print-sums", "When printing the tree structure, additionally print the hash of every file")
  ;

  p_opts_all.add(p_opts_mode).add(p_opts_required).add(p_opts_optional);

  Logger::logLevel() = logInfo;
  Logger::facility() = new LoggerFacilityConsole;
}

options::~options() {}

options& options::getInstance() {
  static options self;
  return self;
}

//returns 0 if ready for operation, 1 on intended quit, 2 on error quit
int options::parse(int argc, char** argv) {
  //make basedir a positional parameter, thus -b/--basedir can be omitted
  positional_options_description pd;
  pd.add("basedir", 1);

  command_line_parser clp(argc, argv);
  clp.options(p_opts_all);
  clp.positional(pd);
  try {
    store(clp.run(), *this);
  } catch (error& e) {
    LOG(logError) << "Error while parsing command line: " << e.what();
    return 2;
  }
  notify();

  if (count("help")) {
      printUsage();
      return 1;
  }

  if (count("version")) {
      printVersion();
      return 1;
  }

  if (setLogLevel())
    return 2;

  // check options compatibility
  operation_t op = opCrawl;
  auto modes = p_opts_mode.options();
  for (auto it = modes.begin(); it != modes.end(); it++) {
    if (count((*it)->long_name())) {
      if (p_operation == opNone)
        p_operation = op;
      else {
        LOG(logError) << "Multiple/incompatible operation modes specified" << std::endl;
        return 2;
      }
    }
    op = (operation_t)((int)op+1);
  }
  if (p_operation == opNone) // if no explicit mode was set
    p_operation = opCrawl; // take opCrawl as default

  //crawl and check modes require a basedir
  if (p_basedir.empty()) {
    if (p_operation == opCrawl || p_operation == opCheck) {
      LOG(logError) << "Operations crawl and check require a basedir";
      printUsage();
      return 2;
    }
  } else //strip trailing slashes, don't try that on an empty basedir string
    while( p_basedir.at(p_basedir.size()-1) == '/' )
      p_basedir.erase(p_basedir.size()-1);

  //check parameters that need a hash algorithm selected
  for(Hasher::hashType_t ht = Hasher::md5; ht < Hasher::hashTypeCount; ht = (Hasher::hashType_t)((int)ht+1)) {
    if (count(Hasher::hashTypeToString(ht))) {
      if (p_hashType != Hasher::noHash) {
        LOG(logError) << "Multiple hashing algorithms specified";
        printUsage();
        return 2;
      }
      p_hashType = ht;
    }
  }

  if( (p_operation == opCheck || OPTS.forceHashing()) && p_hashType == Hasher::noHash ) {
    LOG(logError) << "Hashing algorithm required but none selected";
    printUsage();
    return 2;
  }

  if( (OPTS.watch() || OPTS.forceHashing()) && p_operation != opCrawl ) {
    LOG(logError) << "Crawling-dependant job (watching, (forced) hashing) selected without crawl mode";
    printUsage();
    return 2;
  }

  printVersion();
  if (p_hashType != Hasher::noHash) {
    LOG(logDetailed) << "Selected hash type: " << Hasher::hashTypeToString(p_hashType);
  }
  return 0;
}

void options::printUsage() const {
  printVersion();
  LOG(logInfo) << "Usage: fscrawl [MODE] [OPTIONS] [BASEDIR]";
  LOG(logInfo) << p_opts_all << endl;
}

void options::printVersion() const {
  LOG(logInfo) << "fscrawl " << VERSION;
}

int options::setLogLevel() {
  if (empty())
    return 1;

  int logLevel = OPTS["loglevel"].as<int>();
  if (logLevel > 4)
    logLevel = 4;
  Logger::logLevel() = (logLevel_t)logLevel;

  if( count("logfile") ) {
    LoggerFacilityFile* lff = new LoggerFacilityFile;
    if( !lff->openLogFile(OPT_STR("logfile"), false) ) {
      LOG(logError) << "failed to open logfile \"" << OPT_STR("logfile") << '\"';
      return 1;
    }
    delete Logger::facility();
    Logger::facility() = lff;
  }

  return 0;
}
