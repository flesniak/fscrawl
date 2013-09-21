#ifndef LOGGER_H
#define LOGGER_H

#define LOG(level) \
    if (level > Logger::logLevel()) ; \
    else Logger().getLogger(level)

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

enum logLevel_t { logError, logWarning, logInfo, logDetailed, logDebug };

class LoggerFacility;

class Logger
{
public:
  Logger();
  virtual ~Logger();
  std::ostringstream& getLogger(logLevel_t level = logInfo);
  static LoggerFacility*& facility();
  static logLevel_t& logLevel();
  static std::string toString(logLevel_t level);
  static logLevel_t fromString(const std::string& level);
  inline std::string getTime();
protected:
  std::ostringstream os;
};

class LoggerFacility
{
public:
  virtual ~LoggerFacility();
  virtual void output(const std::string& msg) = 0;
};

class LoggerFacilityConsole : public LoggerFacility
{
public:
  virtual ~LoggerFacilityConsole();
  std::ostream& stream();
  void output(const std::string& msg);
};

class LoggerFacilityFile : public LoggerFacility
{
public:
  virtual ~LoggerFacilityFile();
  void closeLogFile();
  bool openLogFile(const std::string& path, bool truncate = false);
  void output(const std::string& msg);

private:
  std::fstream p_fs;
};

#endif //LOGGER_H
