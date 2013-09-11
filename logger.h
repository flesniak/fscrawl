#ifndef LOGGER_H
#define LOGGER_H

#define LOG(level) \
    if (level > Logger::logLevel()) ; \
    else Logger().getLogger(level)

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
  virtual void output(const std::string& msg) = 0;
};

class LoggerFacilityConsole : public LoggerFacility
{
public:
  static std::ostream& stream();
  void output(const std::string& msg);
};

#endif //LOGGER_H
