#include "logger.h"

#include <ctime>

Logger::Logger()
{
}

std::ostringstream& Logger::getLogger(logLevel_t level)
{
  os << getTime();
  os << " " << toString(level) << ": ";
  os << std::string(level > logDebug ? level - logDebug : 0, '\t');
  return os;
}

Logger::~Logger()
{
  os << std::endl;
  facility()->output(os.str());
}

LoggerFacility*& Logger::facility()
{
  static LoggerFacility* lf = 0;
  return lf;
}

logLevel_t Logger::fromString(const std::string& level)
{
    if (level == "DEBUG")
        return logDebug;
    if (level == "DETAIL")
        return logDetailed;
    if (level == "INFO")
        return logInfo;
    if (level == "WARNING")
        return logWarning;
    if (level == "ERROR")
        return logError;
    Logger().getLogger(logWarning) << "Unknown logging level '" << level << "'. Using INFO level as default.";
    return logInfo;
}

inline std::string Logger::getTime()
{
  char buffer[11];
  time_t t = time(0);
  strftime(buffer, sizeof(buffer), "%X", localtime(&t));
  return buffer;
}

logLevel_t& Logger::logLevel()
{
  static logLevel_t reportingLevel = logDebug;
  return reportingLevel;
}

std::string Logger::toString(logLevel_t level)
{
  static const char* const buffer[] = { "ERROR", "WARNING", "INFO", "DETAIL", "DEBUG" };
  return buffer[level];
}

LoggerFacility::~LoggerFacility()
{
}

LoggerFacilityConsole::~LoggerFacilityConsole()
{
}

inline std::ostream& LoggerFacilityConsole::stream()
{
  static std::ostream* pStream = &std::cerr;
  return *pStream;
}

inline void LoggerFacilityConsole::output(const std::string& msg)
{
  stream() << msg;
}

LoggerFacilityFile::~LoggerFacilityFile()
{
  if( p_fs.is_open() )
    p_fs.close();
}

void LoggerFacilityFile::closeLogFile()
{
  if( p_fs.is_open() )
    p_fs.close();
}

bool LoggerFacilityFile::openLogFile(const std::string& path, bool truncate)
{
  closeLogFile();
  if( truncate )
    p_fs.open(path.c_str(), std::ios_base::out | std::ios_base::trunc);
  else
    p_fs.open(path.c_str(), std::ios_base::out | std::ios_base::app);
  return p_fs.is_open();
}

void LoggerFacilityFile::output(const std::string& msg)
{
  p_fs << msg;
  p_fs.flush();
}
