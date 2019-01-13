#ifndef OPTIONS_H
#define OPTIONS_H

#include <boost/program_options.hpp>
#include <string>

#include "hasher.h"

#define OPTS (options::getInstance())
#define OPT_STR(x) (options::getInstance()[x].as<string>())

class options : public boost::program_options::variables_map {
public:
  ~options();
  static options& getInstance();
  int parse(int argc, char** argv);
  void printUsage() const;
  void printVersion() const;

  bool verifyTree() const { return count("verify"); };
  bool hashCheck() const { return count("check"); };
  bool forceHashing() const { return count("force-hashing"); };
  bool watch() const { return count("watch"); };
  const string& basedir() const { return p_basedir; };
  Hasher::hashType_t hashType() const { return p_hashType; };
  bool allowEmpty() const { return count("allow-empty"); };
  bool dryRun() const { return count("dry-run"); };

  enum operation_t { opNone, opCrawl, opCheck, opVerify, opPrint, opClear, opPurge };
  operation_t getOperation() const { return p_operation; };
private:
  options();
  int setLogLevel();

  boost::program_options::options_description p_opts_mode;
  boost::program_options::options_description p_opts_required;
  boost::program_options::options_description p_opts_optional;
  boost::program_options::options_description p_opts_all;

  Hasher::hashType_t p_hashType;
  string p_basedir;
  operation_t p_operation;
};

#endif //OPTIONS_H
