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

#include <hta/ostream.hpp>

#include <chrono>
#include <ratio>

Db::Db(const std::string& manager_host, const std::string& token)
: metricq::Db(token), signals_(io_service, SIGINT, SIGTERM)
{
    signals_.async_wait([this](auto, auto signal) {
        if (!signal)
        {
            return;
        }
        Log::info() << "Caught signal " << signal << ". Shutdown metricq-db-hta.";
        stop();
    });

    connect(manager_host);
}

void Source::on_error(const std::string& message)
{
    Log::error() << "Connection to MetricQ failed: " << message;
    signals_.cancel();
}

void Source::on_closed()
{
    Log::debug() << "Connection to MetricQ closed.";
    signals_.cancel();
}

void Db::on_db_config(const json& config)
{
    directory = std::make_unique<hta::Directory>(config);
}

void Db::on_db_ready()
{
    assert(directory);
}

void Db::on_data(const std::string& metric_name, const metricq::DataChunk& chunk)
{
    auto begin = std::chrono::system_clock::now();
    Log::trace() << "data_callback with " << chunk.value_size() << " values";
    assert(directory);
    auto metric = (*directory)[metric_name];
    auto range = metric->range();
    uint64_t skip = 0;
    for (TimeValue tv : chunk)
    {
        if (tv.htv.time <= range.second)
        {
            skip++;
            continue;
        }
        try
        {
            metric->insert(tv);
        }
        catch (std::exception& ex)
        {
            Log::fatal() << "failed to insert value for " << metric_name << " ts: " << tv.htv.time
                         << ", value: " << tv.htv.value;
            throw;
        }
    }
    if (skip > 0)
    {
        Log::error() << "Skipped " << skip << " non-monotonic of " << chunk.value_size()
                     << " values";
    }
    metric->flush();
    auto duration = std::chrono::system_clock::now() - begin;
    if (duration > std::chrono::seconds(1))
    {
        Log::warn() << "on_data for " << metric_name << " with " << chunk.value_size()
                    << " entries took "
                    << std::chrono::duration_cast<std::chrono::duration<float>>(duration).count()
                    << " s";
    }
    else
    {
        Log::debug() << "on_data for " << metric_name << " with " << chunk.value_size()
                     << " entries took "
                     << std::chrono::duration_cast<std::chrono::duration<float, std::milli>>(duration).count()
                     << " ms";
    }
}

metricq::HistoryResponse Db::on_history(const std::string& id,
                                        const metricq::HistoryRequest& content)
{
    auto begin = std::chrono::system_clock::now();

    metricq::HistoryResponse response;
    response.set_metric(id);

    Log::trace() << "on_history get metric";
    auto metric = (*directory)[id];

    hta::TimePoint start_time(hta::duration_cast(std::chrono::nanoseconds(content.start_time())));
    hta::TimePoint end_time(hta::duration_cast(std::chrono::nanoseconds(content.end_time())));
    auto interval_ns = hta::duration_cast(std::chrono::nanoseconds(content.interval_ns()));

    Log::trace() << "on_history get data";
    auto rows = metric->retrieve(start_time, end_time, interval_ns);
    Log::trace() << "on_history got data";

    hta::TimePoint last_time;
    Log::trace() << "on_history build response";
    for (auto row : rows)
    {
        auto time_delta =
            std::chrono::duration_cast<std::chrono::nanoseconds>(row.time - last_time);
        response.add_time_delta(time_delta.count());
        response.add_value_min(row.aggregate.minimum);
        response.add_value_max(row.aggregate.maximum);
        response.add_value_avg(row.aggregate.mean());
        last_time = row.time;
    }

    auto duration = std::chrono::system_clock::now() - begin;
    if (duration > std::chrono::seconds(1))
    {
        Log::warn() << "on_history for " << id << "(," << content.start_time() << ","
                    << content.end_time() << "," << content.interval_ns() << ") took "
                    << std::chrono::duration_cast<std::chrono::duration<float>>(duration).count()
                    << " s";
    }
    else
    {
        Log::debug() << "on_history for " << id << "(," << content.start_time() << ","
                     << content.end_time() << "," << content.interval_ns() << ") took "
                     << std::chrono::duration_cast<std::chrono::duration<float, std::milli>>(duration).count()
                     << " ms";
    }
    return response;
}
