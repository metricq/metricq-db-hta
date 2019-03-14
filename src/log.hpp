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
#pragma once

#include <nitro/log/attribute/jiffy.hpp>
#include <nitro/log/attribute/severity.hpp>
#include <nitro/log/filter/severity_filter.hpp>
#include <nitro/log/log.hpp>
#include <nitro/log/sink/stdout.hpp>

namespace detail
{
using record = nitro::log::record<nitro::log::tag_attribute, nitro::log::message_attribute,
                                  nitro::log::severity_attribute, nitro::log::jiffy_attribute>;

template <typename Record>
class log_formater
{
public:
    std::string format(Record& r)
    {
        std::stringstream s;

        s << "[" << r.jiffy() << "][";

        if (!r.tag().empty())
        {
            s << r.tag() << "][";
        }

        s << r.severity() << "]: " << r.message() << '\n';

        return s.str();
    }
};

template <typename Record>
using log_filter = nitro::log::filter::severity_filter<Record>;
} // namespace detail

// TODO different levels should go to stderr/stdout
using Log = nitro::log::logger<detail::record, detail::log_formater, nitro::log::sink::StdOut,
                               detail::log_filter>;

inline void set_severity(nitro::log::severity_level level)
{
    nitro::log::filter::severity_filter<detail::record>::set_severity(level);
}

void initialize_logger();
