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
            s << r.tag() << "[";
        }

        s << r.severity() << "]: " << r.message() << '\n';

        return s.str();
    }
};

template <typename Record>
using log_filter = nitro::log::filter::severity_filter<Record>;
} // namespace detail

using Log = nitro::log::logger<detail::record, detail::log_formater, nitro::log::sink::StdOut,
                               detail::log_filter>;

inline void set_severity(nitro::log::severity_level level)
{
    nitro::log::filter::severity_filter<detail::record>::set_severity(level);
}
