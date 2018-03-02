#include "db.hpp"

#include <nitro/broken_options/parser.hpp>

#include <iostream>

int main(int argc, char* argv[])
{
    nitro::broken_options::parser parser;
    parser.option("server", "The dataheap2 management server to connect to.")
        .default_value("amqp://localhost")
        .short_name("s");
    parser
        .option("token",
                "The token used for source authentification against the dataheap2 manager.")
        .default_value("htaDb");
    parser.toggle("help").short_name("h");

    try
    {
        auto options = parser.parse(argc, argv);

        if (options.given("help"))
        {
            parser.usage();
            return 0;
        }

        Db db(options.get("server"), options.get("token"));
        db.main_loop();
    }
    catch (nitro::broken_options::parser_error& e)
    {
        std::cerr << e.what() << std::endl;
        parser.usage();
        return 1;
    }
    catch (std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}
