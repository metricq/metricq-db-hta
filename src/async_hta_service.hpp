// Copyright (c) 2019, ZIH,
// Technische Universitaet Dresden,
// Federal Republic of Germany
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
#pragma once

#include "db_stats.hpp"
#include "log.hpp"
#include "read_write_stats.hpp"

#include <hta/directory.hpp>
#include <hta/hta.hpp>

#include <metricq/chrono.hpp>
#include <metricq/datachunk.pb.h>
#include <metricq/json.hpp>
#include <metricq/types.hpp>

#include <asio.hpp>

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <cassert>
#include <cmath>

using metricq::Clock;
using metricq::json;
using metricq::TimePoint;

// "File is too complex to perform data-flow analysis" is probably not a good sign
struct TimeValue
{
    TimeValue(metricq::TimeValue dtv)
    : htv{ hta::TimePoint(hta::duration_cast(dtv.time.time_since_epoch())), dtv.value }
    {
    }

    operator hta::TimeValue() const
    {
        return htv;
    }

    hta::TimeValue htv;
};

struct LoggingConfig
{
    LoggingConfig() = default;

    LoggingConfig(const metricq::json& config)
    {
        try
        {
            auto logging = config.at("logging");
            nan_values = logging.value("nan_values", nan_values);
            non_monotonic_values = logging.value("non_monotonic_values", non_monotonic_values);
        }
        catch (std::exception& e)
        {
            Log::info() << "Couldn't parse logging section of the config: " << e.what();
        }
    }

    bool nan_values = true;
    bool non_monotonic_values = true;
};

// Most of the big methods are templated due to the Handler callback type, so this is head-only
class AsyncHtaService
{
public:
    AsyncHtaService()
    {
    }

    ~AsyncHtaService()
    {
        if (pool_)
        {
            pool_->join();
        }
    }

    void register_input_mapping_(const std::string& input, const std::string& name)
    {
        if (auto it_found = mapped_metrics_.find(name); it_found != mapped_metrics_.end())
        {
            Log::fatal() << "trying to map to a metric multiple times, input: " << input
                         << ", name: " << name;
            throw std::logic_error("ambiguous input, invalid configuration.");
        }

        auto [it, inserted] = input_mapping_.emplace(input, name);
        if (!inserted)
        {
            Log::fatal() << "trying to insert the same input name twice: " << input;
            throw std::logic_error("duplicated input, invalid configuration.");
        }
        mapped_metrics_.emplace(name);
    }

    std::string get_mapped_name_(const std::string& input)
    {
        std::lock_guard<std::mutex> guard(mapping_lock_);
        if (auto it = input_mapping_.find(input); it != input_mapping_.end())
        {
            return it->second;
        }
        register_input_mapping_(input, input);
        return input;
    }

    template <class Handler>
    void async_config(const json& config, Handler handler)
    {
        // TODO break down this method to smaller pieces
        int threads = config.at("threads");

        const auto& metrics = config.at("metrics");
        if (!metrics.is_object())
        {
            throw std::runtime_error("configuration error, metrics entry must be an object");
        }
        for (const auto& elem : metrics.items())
        {
            const auto& metric_config = elem.value();
            if (metric_config.count("prefix") && metric_config.at("prefix").get<bool>())
            {
                // Not supported by db.subscribe in the manager at the moment
                throw std::runtime_error("adding prefix metrics no longer supported");
            }
        }

        logging_ = LoggingConfig{ config };

        if (!pool_)
        {
            // Initial configure
            if (threads < 1)
            {
                throw std::runtime_error("invalid number of worker threads configured");
            }
            pool_ = std::make_unique<asio::thread_pool>(threads);
            pool_threads_ = threads;

            auto work = asio::make_work_guard(handler);
            asio::post(*pool_, [this, config, work, handler = std::move(handler)]() mutable {
                Log::info() << "setting up HTA::Directory";
                std::lock_guard<std::mutex> guard(mapping_lock_);

                assert(!directory);
                directory = std::make_unique<hta::Directory>(config, true);

                // setup special write mapping
                const auto& metrics = config.at("metrics");
                for (const auto& elem : metrics.items())
                {
                    std::string name = elem.key();
                    const auto& metric_config = elem.value();
                    auto input = name;
                    if (metric_config.count("input"))
                    {
                        input = metric_config.at("input").get<std::string>();
                    }
                    register_input_mapping_(input, name);
                }

                Log::debug() << "async directory complete";
                handler(get_subscribe_metrics());
            });
        }
        else
        {
            // Reconfigure
            if (threads != this->pool_threads_)
            {
                throw std::runtime_error("changing the number of threads with reconfigure is not "
                                         "supported, restarting");
            }
            // Careful, this is tricky
            // We can't just remake the entire directory, this would mess with in-flight operations
            // But it's also easy to get into a deadlock situation if we try to make a big r/w lock
            // So for now, we only support *adding* metrics. This should be fine.
            Log::debug() << "updated config received, posting to async handler";
            auto work = asio::make_work_guard(handler);
            asio::post(*pool_, [this, config, work, handler = std::move(handler)]() mutable {
                std::lock_guard<std::mutex> guard(mapping_lock_);
                Log::info() << "handling dynamic reconfiguration";
                const auto& metrics = config.at("metrics");
                for (const auto& elem : metrics.items())
                {
                    std::string name = elem.key();
                    const auto& metric_config = elem.value();

                    auto input = name;
                    if (metric_config.count("input"))
                    {
                        input = metric_config.at("input").get<std::string>();
                    }
                    if (mapped_metrics_.count(name))
                    {
                        // metric already defined
                        // TODO check for consistent input mapping
                        continue;
                    }

                    Log::info() << "adding new metric " << name;
                    directory->emplace(name, metric_config);
                    register_input_mapping_(input, name);
                }
                handler(get_subscribe_metrics());
            });
        }
    }

private:
    template <typename Handler>
    void write_(const std::string& id, const metricq::DataChunk& chunk, TimePoint pending_since,
                Handler handler)
    {

        auto begin = std::chrono::system_clock::now();
        stats_.write_active(begin - pending_since);

        assert(directory);
        auto& metric = (*directory)[id];
        auto max_ts = metric.range().second;
        uint64_t skip_non_monotonic = 0;
        uint64_t skip_nan = 0;
        for (TimeValue tv : chunk)
        {
            if (tv.htv.time <= max_ts)
            {
                skip_non_monotonic++;
                continue;
            }
            if (std::isnan(tv.htv.value))
            {
                skip_nan++;
                continue;
            }
            max_ts = tv.htv.time;
            try
            {
                metric.insert(tv);
            }
            catch (std::exception& ex)
            {
                Log::fatal() << "[" << id << "] failed to insert ts: " << tv.htv.time
                             << ", value: " << tv.htv.value;
                throw;
            }
        }
        if (logging_.non_monotonic_values && skip_non_monotonic > 0)
        {
            Log::warn() << "[" << id << "] skipped " << skip_non_monotonic << " non-monotonic of "
                        << chunk.value_size() << " values";
        }
        if (logging_.nan_values && skip_nan > 0)
        {
            Log::warn() << "[" << id << "] skipped " << skip_nan << " NaNs of "
                        << chunk.value_size() << " values";
        }

        metric.flush();
        auto duration = std::chrono::system_clock::now() - begin;
        if (duration > std::chrono::seconds(1))
        {
            Log::warn()
                << "[" << id << "] on_data with " << chunk.value_size() << " entries took "
                << std::chrono::duration_cast<std::chrono::duration<float>>(duration).count()
                << " s";
        }
        else
        {
            Log::debug() << "[" << id << "] on_data with " << chunk.value_size() << " entries took "
                         << std::chrono::duration_cast<std::chrono::duration<float, std::milli>>(
                                duration)
                                .count()
                         << " ms";
        }

        // We compute raw size of TimeValues and ignore skipped elements for now
        size_t data_size = chunk.value_size() * sizeof(TimeValue);
        stats_.write_complete(duration, data_size);
        handler();
    }

public:
    template <class Handler>
    void async_write(const std::string& input, const metricq::DataChunk& chunk, Handler handler)
    {
        // note we copy the chunk here as its a reused buffer owned by the original sink
        std::string name = get_mapped_name_(input);

        auto pending_since = Clock::now();
        stats_.write_pending();
        asio::post(get_strand(name),
                   [this, name, chunk, pending_since, handler = std::move(handler)]() mutable {
                       this->write_(name, chunk, pending_since, std::move(handler));
                   });
    }

private:
    template <typename Handler>
    void read_(const std::string& id, const metricq::HistoryRequest& content,
               TimePoint pending_since, Handler& handler)
    {
        auto begin = Clock::now();
        stats_.read_active(begin - pending_since);

        metricq::HistoryResponse response;
        response.set_metric(id);

        Log::trace() << "on_history get metric";
        auto& metric = (*directory)[id];

        size_t data_size = 0;
        switch (content.type())
        {
        case metricq::HistoryRequest::AGGREGATE_TIMELINE:
        {
            hta::TimePoint start_time(
                hta::duration_cast(std::chrono::nanoseconds(content.start_time())));
            hta::TimePoint end_time(
                hta::duration_cast(std::chrono::nanoseconds(content.end_time())));
            auto interval_max =
                hta::duration_cast(std::chrono::nanoseconds(content.interval_max()));

            Log::trace() << "on_history get data";
            auto rows = metric.retrieve(start_time, end_time, interval_max);
            Log::trace() << "on_history got data";

            hta::TimePoint last_time;
            Log::trace() << "on_history build response";
            for (auto row : rows)
            {
                auto time_delta =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(row.time - last_time);
                response.add_time_delta(time_delta.count());
                auto aggregate = response.add_aggregate();
                aggregate->set_minimum(row.aggregate.minimum);
                aggregate->set_maximum(row.aggregate.maximum);
                aggregate->set_sum(row.aggregate.sum);
                aggregate->set_count(row.aggregate.count);
                aggregate->set_integral(row.aggregate.integral);
                aggregate->set_active_time(row.aggregate.active_time.count());
                last_time = row.time;
            }
            data_size = sizeof(decltype(rows)::value_type) * rows.size();
        }
        break;
        case metricq::HistoryRequest::FLEX_TIMELINE:
        {
            hta::TimePoint start_time(
                hta::duration_cast(std::chrono::nanoseconds(content.start_time())));
            hta::TimePoint end_time(
                hta::duration_cast(std::chrono::nanoseconds(content.end_time())));
            auto interval_max =
                hta::duration_cast(std::chrono::nanoseconds(content.interval_max()));

            Log::trace() << "on_history get data";
            auto flex = metric.retrieve_flex(start_time, end_time, interval_max);
            Log::trace() << "on_history got data";

            hta::TimePoint last_time;
            if (auto rows_p = std::get_if<std::vector<hta::Row>>(&flex))
            {
                for (auto row : *rows_p)
                {
                    auto time_delta =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(row.time - last_time);
                    response.add_time_delta(time_delta.count());
                    auto aggregate = response.add_aggregate();
                    aggregate->set_minimum(row.aggregate.minimum);
                    aggregate->set_maximum(row.aggregate.maximum);
                    aggregate->set_sum(row.aggregate.sum);
                    aggregate->set_count(row.aggregate.count);
                    aggregate->set_integral(row.aggregate.integral);
                    aggregate->set_active_time(row.aggregate.active_time.count());
                    last_time = row.time;
                }
                data_size = sizeof(hta::Row) * rows_p->size();
            }
            else
            {
                const auto& rows = std::get<std::vector<hta::TimeValue>>(flex);
                for (auto tv : rows)
                {
                    auto time_delta =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(tv.time - last_time);
                    response.add_time_delta(time_delta.count());
                    response.add_value(tv.value);
                    last_time = tv.time;
                }
                data_size = sizeof(hta::TimeValue) * rows.size();
            }

            Log::trace() << "on_history build response";
        }
        break;
        case metricq::HistoryRequest::AGGREGATE:
        {
            hta::TimePoint start_time(
                hta::duration_cast(std::chrono::nanoseconds(content.start_time())));
            hta::TimePoint end_time(
                hta::duration_cast(std::chrono::nanoseconds(content.end_time())));

            Log::trace() << "on_history get data";
            auto data = metric.aggregate(start_time, end_time);
            Log::trace() << "on_history got data";

            Log::trace() << "on_history build response";
            auto aggregate = response.add_aggregate();
            aggregate->set_minimum(data.minimum);
            aggregate->set_maximum(data.maximum);
            aggregate->set_sum(data.sum);
            aggregate->set_count(data.count);
            aggregate->set_integral(data.integral);
            aggregate->set_active_time(data.active_time.count());
            response.add_time_delta(start_time.time_since_epoch().count());
            data_size = sizeof(aggregate);
        }
        break;
        case metricq::HistoryRequest::LAST_VALUE:
        {
            Log::trace() << "on_history get data";
            auto ts = hta::TimePoint(hta::Duration(std::numeric_limits<int64_t>::max()));
            auto data = metric.retrieve(ts, ts, { hta::Scope::extended, hta::Scope::open });
            Log::trace() << "on_history got data";

            Log::trace() << "on_history build response";
            if (data.size() == 1)
            {
                auto tv = data.back();

                response.add_time_delta(tv.time.time_since_epoch().count());
                response.add_value(tv.value);
                data_size = sizeof(tv);
            }
            else if (data.size() > 1)
            {
                Log::warn() << "Retrieved more than one TimeValue when trying to get last data "
                               "point in metric '"
                            << id << "'";
            }
        }
        break;
        default:
            Log::warn() << "got unknown HistoryRequest type";
        }

        auto duration = std::chrono::system_clock::now() - begin;
        if (duration > std::chrono::seconds(1))
        {
            Log::warn()
                << "on_history for " << id << "(," << content.start_time() << ","
                << content.end_time() << "," << content.interval_max() << ") took "
                << std::chrono::duration_cast<std::chrono::duration<float>>(duration).count()
                << " s";
        }
        else
        {
            Log::debug() << "on_history for " << id << "(," << content.start_time() << ","
                         << content.end_time() << "," << content.interval_max() << ") took "
                         << std::chrono::duration_cast<std::chrono::duration<float, std::milli>>(
                                duration)
                                .count()
                         << " ms";
        }
        stats_.read_complete(duration, data_size);
        handler(response);
    }

public:
    template <class Handler>
    void async_read(const std::string& id, const metricq::HistoryRequest& content, Handler handler)
    {
        stats_.read_pending();
        auto pending_since = Clock::now();

        asio::post(get_strand(id), [this, id, content, pending_since,
                                    handler = std::move(handler)]() mutable {
            try
            {
                this->read_(id, content, pending_since, handler);
            }
            catch (std::exception& e)
            {
                Log::error()
                    << "An error occurred during the handling of a history request for metricq '"
                    << id << "': " << e.what();

                handler.failed(id, e.what());
            }
        });
    }

private:
    asio::strand<asio::thread_pool::executor_type>& get_strand(const std::string& id)
    {
        assert(directory);
        std::lock_guard<std::mutex> guard(strand_lock_);
        auto it = strands_.try_emplace(id, pool_->get_executor());
        return it.first->second;
    }

    json get_subscribe_metrics() const
    {
        // assumes there already is a mapping_lock_
        json ret = json::array();
        for (const auto& elem : input_mapping_)
        {
            ret.push_back(json{ { "input", elem.first }, { "name", elem.second } });
        }
        return ret;
    }

public:
    DbStats& stats()
    {
        return stats_;
    }

private:
    std::unique_ptr<hta::Directory> directory;
    std::mutex mapping_lock_;
    /**
     * mapping from a input metric name to a actual logical metric name
     * e.g. foo.bar.power.100Hz => foo.bar.power
     */
    std::unordered_map<std::string, std::string> input_mapping_;
    /**
     * set of logical metric names that are already included in the input mapping
     * used to avoid ambiguous mappings
     */
    std::unordered_set<std::string> mapped_metrics_;
    std::mutex strand_lock_;
    int pool_threads_ = 0;
    std::unique_ptr<asio::thread_pool> pool_;
    std::map<std::string, asio::strand<asio::thread_pool::executor_type>> strands_;

    DbStats stats_;
    LoggingConfig logging_;
};
