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
#include "read_write_stats.hpp"

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
        stats_.pending_count_++;
    }

    template <typename T>
    void active(T pending_duration)
    {
        std::lock_guard lock(stats_mutex_);
        stats_.pending_duration_ += std::chrono::duration_cast<metricq::Duration>(pending_duration);
        stats_.pending_count_--;
        stats_.active_count_++;
    }

    template <typename T>
    void complete(T active_duration, size_t data_size)
    {
        std::lock_guard lock(stats_mutex_);
        stats_.ready_count_++;
        stats_.active_count_--;
        stats_.active_duration_ += std::chrono::duration_cast<metricq::Duration>(active_duration);
        stats_.data_size_ += data_size;
    }

    struct Stats
    {
        size_t ready_count_ = 0;
        size_t data_size_ = 0;
        metricq::Duration pending_duration_ = metricq::Duration(0);
        metricq::Duration active_duration_ = metricq::Duration(0);
        size_t pending_count_ = 0;
        size_t active_count_ = 0;
    };

    Stats collect()
    {
        std::lock_guard lock(stats_mutex_);
        Stats collected_stats = stats_;
        stats_ = Stats();
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
      active_time_(writer.output_metric(prefix + read_or_write + ".active.time")),
      pending_count_(writer.output_metric(prefix + read_or_write + ".pending.count")),
      active_count_(writer.output_metric(prefix + read_or_write + ".active.count"))
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

        active_time_.metadata.unit("s");
        active_time_.metadata.quantity("time");
        active_time_.metadata.description(
            fmt::format("average time {}-requests were being processed", read_or_write));
        active_time_.metadata.scope(metricq::Metadata::Scope::last);
        active_time_.metadata.rate(rate);

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
    }

    void write(StatsCollector::Stats stats, metricq::TimePoint time, double duration)
    {
        assert(duration > 0);
        request_rate_.send({ time, stats.ready_count_ / duration });
        data_rate_.send({ time, stats.data_size_ / duration });
        pending_time_.send({ time, std::chrono::duration_cast<std::chrono::duration<double>>(
                                       stats.pending_duration_)
                                       .count() });
        active_time_.send({ time, std::chrono::duration_cast<std::chrono::duration<double>>(
                                      stats.active_duration_)
                                      .count() });
        pending_count_.send({ time, static_cast<double>(stats.pending_count_) });
        active_count_.send({ time, static_cast<double>(stats.active_count_) });
    }

private:
    Metric& request_rate_;
    Metric& data_rate_;
    Metric& pending_time_;
    Metric& active_time_;
    Metric& pending_count_;
    Metric& active_count_;
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
        read_metrics_.write(read.collect(), time, duration);
        write_metrics_.write(write.collect(), time, duration);
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
    assert(!impl);
    impl = std::make_unique<DbStatsImpl>(db, prefix, rate);
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

void DbStats::collect()
{
    if (impl)
    {
        impl->collect();
    }
}