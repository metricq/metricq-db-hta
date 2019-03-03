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

#include "log.hpp"

#include <hta/directory.hpp>
#include <hta/hta.hpp>

#include <metricq/datachunk.pb.h>
#include <metricq/json.hpp>
#include <metricq/types.hpp>

#include <asio.hpp>

#include <cassert>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

using metricq::json;

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

    template <class Handler>
    void async_config(const json& config, Handler handler)
    {
        auto work = asio::make_work_guard(handler);

        int threads = config.at("threads");
        assert(!pool_);
        pool_ = std::make_unique<asio::thread_pool>(threads);
        asio::post(*pool_, [this, config, work, handler = std::move(handler)]() mutable {
            Log::debug() << "async directory setup";
            directory = std::make_unique<hta::Directory>(config, true);
            Log::debug() << "async directory complete";
            dispatch(work.get_executor(), [handler = std::move(handler)]() mutable { handler(); });
        });
    }

private:
    template <typename Work, typename Handler>
    void write_(const std::string& id, Work work, const metricq::DataChunk& chunk, Handler handler)
    {

        auto begin = std::chrono::system_clock::now();

        assert(directory);
        auto& metric = (*directory)[id];
        auto max_ts = metric.range().second;
        uint64_t skip = 0;
        for (TimeValue tv : chunk)
        {
            if (tv.htv.time <= max_ts)
            {
                skip++;
                continue;
            }
            max_ts = tv.htv.time;
            try
            {
                metric.insert(tv);
            }
            catch (std::exception& ex)
            {
                Log::fatal() << "failed to insert value for " << id << " ts: " << tv.htv.time
                             << ", value: " << tv.htv.value;
                throw;
            }
        }
        if (skip > 0)
        {
            Log::error() << "Skipped " << skip << " non-monotonic of " << chunk.value_size()
                         << " values";
        }
        metric.flush();
        auto duration = std::chrono::system_clock::now() - begin;
        if (duration > std::chrono::seconds(1))
        {
            Log::warn()
                << "on_data for " << id << " with " << chunk.value_size() << " entries took "
                << std::chrono::duration_cast<std::chrono::duration<float>>(duration).count()
                << " s";
        }
        else
        {
            Log::debug() << "on_data for " << id << " with " << chunk.value_size()
                         << " entries took "
                         << std::chrono::duration_cast<std::chrono::duration<float, std::milli>>(
                                duration)
                                .count()
                         << " ms";
        }

        dispatch(work.get_executor(), [handler = std::move(handler)]() mutable { handler(); });
    }

public:
    template <class Handler>
    void async_write(const std::string& id, const metricq::DataChunk& chunk, Handler handler)
    {
        auto& strand = get_strand(id);
        auto work = asio::make_work_guard(handler);

        // note we copy the chunk here as its a reused buffer owned by the original sink
        asio::post(strand, [this, id, work, chunk, handler = std::move(handler)]() mutable {
            this->write_(id, work, chunk, std::move(handler));
        });
    }

private:
    template <typename Work, typename Handler>
    void read_(const std::string& id, Work work, const metricq::HistoryRequest& content,
               Handler handler)
    {
        auto begin = std::chrono::system_clock::now();

        metricq::HistoryResponse response;
        response.set_metric(id);

        Log::trace() << "on_history get metric";
        auto& metric = (*directory)[id];

        hta::TimePoint start_time(
            hta::duration_cast(std::chrono::nanoseconds(content.start_time())));
        hta::TimePoint end_time(hta::duration_cast(std::chrono::nanoseconds(content.end_time())));
        auto interval_ns = hta::duration_cast(std::chrono::nanoseconds(content.interval_ns()));

        Log::trace() << "on_history get data";
        auto rows = metric.retrieve(start_time, end_time, interval_ns);
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
            Log::warn()
                << "on_history for " << id << "(," << content.start_time() << ","
                << content.end_time() << "," << content.interval_ns() << ") took "
                << std::chrono::duration_cast<std::chrono::duration<float>>(duration).count()
                << " s";
        }
        else
        {
            Log::debug() << "on_history for " << id << "(," << content.start_time() << ","
                         << content.end_time() << "," << content.interval_ns() << ") took "
                         << std::chrono::duration_cast<std::chrono::duration<float, std::milli>>(
                                duration)
                                .count()
                         << " ms";
        }
        dispatch(work.get_executor(),
                 [handler = std::move(handler), response = std::move(response)]() mutable {
                     handler(std::move(response));
                 });
    }

public:
    template <class Handler>
    void async_read(const std::string id, const metricq::HistoryRequest& content, Handler handler)
    {
        auto& strand = get_strand(id);
        auto work = asio::make_work_guard(handler);

        asio::post(strand, [this, id, work, content, handler = std::move(handler)]() mutable {
            this->read_(id, work, content, std::move(handler));
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

private:
    std::unique_ptr<hta::Directory> directory;
    std::mutex strand_lock_;
    std::map<std::string, asio::strand<asio::thread_pool::executor_type>> strands_;
    std::unique_ptr<asio::thread_pool> pool_;
};