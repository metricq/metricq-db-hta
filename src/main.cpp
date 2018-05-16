#include "db.hpp"
#include "log.hpp"

#include <nitro/broken_options/parser.hpp>

#include <iostream>

int main(int argc, char* argv[])
{
    set_severity(nitro::log::severity_level::info);

    nitro::broken_options::parser parser;
    parser.option("server", "The dataheap2 management server to connect to.")
        .default_value("amqp://localhost")
        .short_name("s");
    parser
        .option("token", "The token used for source authentication against the dataheap2 manager.")
        .default_value("htaDb");
    parser.toggle("verbose").short_name("v");
    parser.toggle("quiet").short_name("q");
    parser.toggle("help").short_name("h");

    try
    {
        auto options = parser.parse(argc, argv);

        if (options.given("help"))
        {
            parser.usage();
            return 0;
        }

        if (options.given("verbose"))
        {
            set_severity(nitro::log::severity_level::debug);
        }
        else if (options.given("quiet"))
        {
            set_severity(nitro::log::severity_level::warn);
        }

        initialize_logger();
        Db db(options.get("server"), options.get("token"));
        db.main_loop();
        Log::info() << "exiting main loop.";
    }
    catch (nitro::broken_options::parser_error& e)
    {
        Log::warn() << e.what();
        parser.usage();
        return 1;
    }
    catch (std::exception& e)
    {
        Log::error() << "Unhandled exception: " << e.what();
    }

    return 0;
}
