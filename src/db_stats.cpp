// metricq-db-hta
// Copyright (C) 2021 ZIH, Technische Universitaet Dresden, Federal Republic of Germany
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

#include "db_stats.hpp"

#include "db.hpp"
#include "log.hpp"

#include <metricq/chrono.hpp>
#include <metricq/metadata.hpp>
#include <metricq/metric.hpp>

#include <fmt/format.h>

#include <chrono>
#include <mutex>

class StatsCollector
{
public:
    StatsCollector()
    {
    }

public:
    void pending()
    {
        std::lock_guard lock(stats_mutex_);
        stats_.in_pending_state_++;
    }

    template <typename T>
    void active(T pending_duration)
    {
        std::lock_guard lock(stats_mutex_);
        stats_.pending_duration_ += std::chrono::duration_cast<metricq::Duration>(pending_duration);
        stats_.in_pending_state_--;
        stats_.started_count_++;
        stats_.in_active_state_++;
    }

    template <typename T>
    void complete(T active_duration, size_t data_size)
    {
        std::lock_guard lock(stats_mutex_);
        stats_.completed_count_++;
        stats_.in_active_state_--;
        stats_.active_duration_ += std::chrono::duration_cast<metricq::Duration>(active_duration);
        stats_.data_size_ += data_size;
    }

    template <typename T>
    void failed(T active_duration)
    {
        std::lock_guard lock(stats_mutex_);
        stats_.in_active_state_--;
        stats_.failed_count_++;
        stats_.active_duration_ += std::chrono::duration_cast<metricq::Duration>(active_duration);
    }

    struct Stats
    {
        size_t completed_count_ = 0;
        size_t failed_count_ = 0;
        size_t started_count_ = 0;
        size_t data_size_ = 0;
        metricq::Duration pending_duration_ = metricq::Duration(0);
        metricq::Duration active_duration_ = metricq::Duration(0);

        // number of requests in pending state, not affected by reset()
        size_t in_pending_state_ = 0;

        // number of requests in active state, not affected by reset()
        size_t in_active_state_ = 0;

        void reset() noexcept
        {
            completed_count_ = 0;
            failed_count_ = 0;
            started_count_ = 0;
            data_size_ = 0;
            pending_duration_ = metricq::Duration(0);
            active_duration_ = metricq::Duration(0);
        }
    };

    Stats collect()
    {
        std::lock_guard lock(stats_mutex_);
        Stats collected_stats = stats_;
        stats_.reset();
        return collected_stats;
    }

private:
    std::mutex stats_mutex_;
    Stats stats_;
};

using Metric = metricq::Metric<metricq::Db>;

class StatsMetrics
{
public:
    StatsMetrics(const std::string& read_or_write, Db& writer, const std::string& prefix,
                 double rate)
    : request_rate_(writer.output_metric(prefix + read_or_write + ".request.rate")),
      data_rate_(writer.output_metric(prefix + read_or_write + ".data.rate")),
      pending_time_(writer.output_metric(prefix + read_or_write + ".pending.time")),
      active_utilization_(writer.output_metric(prefix + read_or_write + ".utilization")),
      pending_count_(writer.output_metric(prefix + read_or_write + ".pending.count")),
      active_count_(writer.output_metric(prefix + read_or_write + ".active.count")),
      failed_count_(writer.output_metric(prefix + read_or_write + ".failed.count"))
    {
        request_rate_.metadata.unit("Hz");
        request_rate_.metadata.quantity("rate");
        request_rate_.metadata.description(
            fmt::format("rate of completed {}-requests", read_or_write));
        request_rate_.metadata.scope(metricq::Metadata::Scope::last);
        request_rate_.metadata.rate(rate);

        data_rate_.metadata.unit("B/s");
        data_rate_.metadata.quantity("rate");
        data_rate_.metadata.description(fmt::format("data rate for {} payload", read_or_write));
        data_rate_.metadata.scope(metricq::Metadata::Scope::last);
        data_rate_.metadata.rate(rate);

        pending_time_.metadata.unit("s");
        pending_time_.metadata.quantity("time");
        pending_time_.metadata.description(
            fmt::format("average time {}-requests were pending", read_or_write));
        pending_time_.metadata.scope(metricq::Metadata::Scope::last);
        pending_time_.metadata.rate(rate);

        active_utilization_.metadata.unit("");
        active_utilization_.metadata.quantity("utilization");
        active_utilization_.metadata.description(
            fmt::format("fraction of time spent on processing {}-requests", read_or_write));
        active_utilization_.metadata.scope(metricq::Metadata::Scope::last);
        active_utilization_.metadata.rate(rate);

        pending_count_.metadata.unit("");
        pending_count_.metadata.quantity("");
        pending_count_.metadata.description(
            fmt::format("number of pending {}-requests", read_or_write));
        pending_count_.metadata.scope(metricq::Metadata::Scope::point);
        pending_count_.metadata.rate(rate);

        active_count_.metadata.unit("");
        active_count_.metadata.quantity("");
        active_count_.metadata.description(
            fmt::format("number of actively processed {}-requests", read_or_write));
        active_count_.metadata.scope(metricq::Metadata::Scope::point);
        active_count_.metadata.rate(rate);

        failed_count_.metadata.unit("");
        failed_count_.metadata.quantity("");
        failed_count_.metadata.description(
            fmt::format("number of failed {}-requests", read_or_write));
        failed_count_.metadata.scope(metricq::Metadata::Scope::last);
        failed_count_.metadata.rate(rate);
    }

    void write(StatsCollector::Stats stats, metricq::TimePoint time, double duration)
    {
        assert(duration > 0);
        request_rate_.send({ time, stats.completed_count_ / duration });
        data_rate_.send({ time, stats.data_size_ / duration });
        double pending_time = 0;
        if (stats.started_count_ > 0)
        {
            pending_time =
                std::chrono::duration_cast<std::chrono::duration<double>>(stats.pending_duration_)
                    .count() /
                stats.started_count_;
        }
        else
        {
            assert(stats.pending_duration_.count() == 0);
        }
        pending_time_.send({ time, pending_time });
        active_utilization_.send({ time, std::chrono::duration_cast<std::chrono::duration<double>>(
                                             stats.active_duration_)
                                                 .count() /
                                             duration });
        pending_count_.send({ time, static_cast<double>(stats.in_pending_state_) });
        active_count_.send({ time, static_cast<double>(stats.in_active_state_) });
        failed_count_.send({ time, static_cast<double>(stats.failed_count_) });
    }

private:
    Metric& request_rate_;
    Metric& data_rate_;
    Metric& pending_time_;
    Metric& active_utilization_;
    Metric& pending_count_;
    Metric& active_count_;
    Metric& failed_count_;
};

class DbStats::DbStatsImpl
{
public:
    DbStatsImpl(Db& db, const std::string& prefix, double rate)
    : previous_collect_time_(metricq::Clock::now()), read_metrics_("read", db, prefix, rate),
      write_metrics_("write", db, prefix, rate)
    {
    }

    void collect()
    {
        auto time = metricq::Clock::now();
        double duration =
            std::chrono::duration_cast<std::chrono::duration<double>>(time - previous_collect_time_)
                .count();
        // collect stats as fast as possible without delaying due to write
        auto read_stats = read.collect();
        auto write_stats = write.collect();
        read_metrics_.write(read_stats, time, duration);
        write_metrics_.write(write_stats, time, duration);
        previous_collect_time_ = time;
    }

    StatsCollector read;
    StatsCollector write;

private:
    metricq::TimePoint previous_collect_time_;
    StatsMetrics read_metrics_;
    StatsMetrics write_metrics_;
};

DbStats::DbStats()
{
}

DbStats::~DbStats()
{
}

void DbStats::init(Db& db, const std::string& prefix, double rate)
{
    if (impl)
    {
        Log::warn() << "Trying to reinitialize DbStats.";
        Log::warn() << "This is not supported, metadata and metric names will not be updated until "
                       "restart.";
        return;
    }
    impl = std::make_unique<DbStatsImpl>(db, prefix, rate);
}

void DbStats::reset()
{
    impl = nullptr;
}

void DbStats::read_pending()
{
    if (impl)
    {
        impl->read.pending();
    }
}

void DbStats::read_active(metricq::Duration pending_duration)
{
    if (impl)
    {
        impl->read.active(pending_duration);
    }
}

void DbStats::read_complete(metricq::Duration active_duration, std::size_t data_size)
{
    if (impl)
    {
        impl->read.complete(active_duration, data_size);
    }
}

void DbStats::read_failed(metricq::Duration active_duration)
{
    if (impl)
    {
        impl->read.failed(active_duration);
    }
}

void DbStats::write_pending()
{
    if (impl)
    {
        impl->write.pending();
    }
}

void DbStats::write_active(metricq::Duration pending_duration)
{
    if (impl)
    {
        impl->write.active(pending_duration);
    }
}

void DbStats::write_complete(metricq::Duration active_duration, std::size_t data_size)
{
    if (impl)
    {
        impl->write.complete(active_duration, data_size);
    }
}

void DbStats::write_failed(metricq::Duration active_duration)
{
    if (impl)
    {
        impl->write.failed(active_duration);
    }
}

void DbStats::collect()
{
    if (impl)
    {
        impl->collect();
    }
}
