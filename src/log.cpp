#include "log.hpp"

#include <metricq/log.hpp>

class Logger : public metricq::Logger
{
public:
    void trace(const std::string& msg) override
    {
        Log::trace("metricq") << msg;
    }

    void debug(const std::string& msg) override
    {
        Log::debug("metricq") << msg;
    }

    void notice(const std::string& msg) override
    {
        Log::info("metricq") << msg;
    }

    void info(const std::string& msg) override
    {
        Log::info("metricq") << msg;
    }

    void warn(const std::string& msg) override
    {
        Log::warn("metricq") << msg;
    }

    void error(const std::string& msg) override
    {
        Log::error("metricq") << msg;
    }

    void fatal(const std::string& msg) override
    {
        Log::fatal("metricq") << msg;
    }
};

void initialize_logger()
{
    metricq::make_logger<Logger>();
}
