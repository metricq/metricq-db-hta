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

#include <metricq/types.hpp>

#include <chrono>
#include <mutex>

class Stats
{
public:
    template <typename T>
    void add_read_duration(T duration)
    {

        std::lock_guard lock(stats_lock_);
        read_count_++;
        read_duration_ +=
            std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
        decrement_ongoing();
        log_stats();
    }

    template <typename T>
    void add_write_duration(T duration)
    {

        std::lock_guard lock(stats_lock_);
        write_count_++;
        write_duration_ +=
            std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
        decrement_ongoing();

        log_stats();
    }

    void increment_ongoing()
    {
        std::lock_guard lock(stats_lock_);
        ongoing_requests_count_++;
        log_stats();
    }

private:
    void decrement_ongoing()
    {
        ongoing_requests_count_--;
    }

    void log_stats()
    {
        auto duration = metricq::Clock::now() - last_log_;

        if (duration > std::chrono::seconds(10))
        {
            Log::info()
                << "read stats: " << read_duration_ << "s for " << read_count_ << " reads, avg "
                << read_duration_ / read_count_ << "s, utilization "
                << read_duration_ /
                       std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
            Log::info()
                << "write stats: " << write_duration_ << "s for " << write_count_ << " writes, avg "
                << write_duration_ / write_count_ << "s, utilization "
                << write_duration_ /
                       std::chrono::duration_cast<std::chrono::duration<double>>(duration).count();
            Log::info() << "ongoing requests: " << ongoing_requests_count_;

            read_duration_ = 0;
            write_duration_ = 0;
            read_count_ = 0;
            write_count_ = 0;
            last_log_ = metricq::Clock::now();
        }
    }

private:
    std::mutex stats_lock_;
    double read_duration_ = 0;
    size_t read_count_ = 0;
    double write_duration_ = 0;
    size_t write_count_ = 0;
    size_t ongoing_requests_count_ = 0;
    metricq::TimePoint last_log_;
};