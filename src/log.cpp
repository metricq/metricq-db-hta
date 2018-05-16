#include "log.hpp"

#include <dataheap2/log.hpp>

class Logger : public dataheap2::Logger
{
public:
    void trace(const std::string& msg) override
    {
        Log::trace("dataheap2") << msg;
    }

    void debug(const std::string& msg) override
    {
        Log::debug("dataheap2") << msg;
    }

    void notice(const std::string& msg) override
    {
        Log::info("dataheap2") << msg;
    }

    void info(const std::string& msg) override
    {
        Log::info("dataheap2") << msg;
    }

    void warn(const std::string& msg) override
    {
        Log::warn("dataheap2") << msg;
    }

    void error(const std::string& msg) override
    {
        Log::error("dataheap2") << msg;
    }

    void fatal(const std::string& msg) override
    {
        Log::fatal("dataheap2") << msg;
    }
};

void initialize_logger()
{
    dataheap2::make_logger<Logger>();
}
