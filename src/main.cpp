// metricq-db-hta
// Copyright (C) 2018 ZIH, Technische Universitaet Dresden, Federal Republic of Germany
//
// All rights reserved.
//
// This file is part of metricq-db-hta.
//
// metricq-db-hta is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// metricq-db-hta is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with metricq-db-hta.  If not, see <http://www.gnu.org/licenses/>.
#include "db.hpp"
#include "log.hpp"

#include <nitro/broken_options/parser.hpp>

#include <iostream>

int main(int argc, char* argv[])
{
    set_severity(nitro::log::severity_level::info);

    nitro::broken_options::parser parser;
    parser.option("server", "The metricq management server to connect to.")
        .default_value("amqp://localhost")
        .short_name("s");
    parser.option("token", "The token used for source authentication against the metricq manager.")
        .default_value("db-hta");
    parser.toggle("trace").short_name("t");
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

        if (options.given("trace"))
        {
            set_severity(nitro::log::severity_level::trace);
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
        Log::info() << "exiting main loop " << metricq::Clock::now().time_since_epoch().count();
    }
    catch (nitro::broken_options::parsing_error& e)
    {
        Log::warn() << e.what();
        parser.usage();
        return 1;
    }
    catch (nitro::broken_options::parser_error& e)
    {
        Log::error() << "broken options are broken " << e.what();
        return 1;
    }
    catch (std::exception& e)
    {
        Log::error() << "Unhandled exception: " << e.what();
        return 2;
    }

    return 0;
}
