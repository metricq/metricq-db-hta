// Copyright (c) 2018, ZIH, Technische Universitaet Dresden, Federal Republic of Germany
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright notice,
//       this list of conditions and the following disclaimer in the documentation
//       and/or other materials provided with the distribution.
//     * Neither the name of metricq nor the names of its contributors
//       may be used to endorse or promote products derived from this software
//       without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#include "db.hpp"

#include "log.hpp"

#include <hta/ostream.hpp>

#include <chrono>

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
                    << "s";
    }
    else
    {
        Log::debug() << "on_data for " << metric_name << " with " << chunk.value_size()
                     << " entries took "
                     << std::chrono::duration_cast<std::chrono::duration<float>>(duration).count()
                     << "s";
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
                    << "s";
    }
    else
    {
        Log::debug() << "on_history for " << id << "(," << content.start_time() << ","
                     << content.end_time() << "," << content.interval_ns() << ") took "
                     << std::chrono::duration_cast<std::chrono::duration<float>>(duration).count()
                     << "s";
    }
    return response;
}
