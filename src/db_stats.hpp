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
#pragma once

#include <metricq/chrono.hpp>
#include <metricq/json.hpp>

#include <cstddef>
#include <memory>

class Db;

class DbStats
{
public:
    DbStats();

    ~DbStats();

    void init(Db& db, const std::string& prefix, double rate);

    void reset();

    void read_pending();

    void read_active(metricq::Duration pending_duration);

    void read_complete(metricq::Duration active_duration, std::size_t data_size);

    void read_failed(metricq::Duration active_duration);

    void write_pending();

    void write_active(metricq::Duration pending_duration);

    void write_complete(metricq::Duration active_duration, std::size_t data_size);

    void write_failed(metricq::Duration active_duration);

    void collect();

private:
    class DbStatsImpl;

    std::unique_ptr<DbStatsImpl> impl;
};

template <void (DbStats::*active)(metricq::Duration), void (DbStats::*failed)(metricq::Duration),
          void (DbStats::*complete)(metricq::Duration, std::size_t)>
class DbStatsTransaction
{
public:
    DbStatsTransaction(DbStats& stats,
                       metricq::TimePoint pending_since)
    : begin_(metricq::Clock::now()), stats_(&stats)
    {
        (stats_->*active)(begin_ - pending_since);
    }

    DbStatsTransaction(const DbStatsTransaction&) = delete;
    DbStatsTransaction& operator=(const DbStatsTransaction&) = delete;

    ~DbStatsTransaction()
    {
        if (!success_)
        {
            (stats_->*failed)(metricq::Clock::now() - begin_);
        }
    }

    metricq::Duration completed(std::size_t data_size)
    {
        auto duration = metricq::Clock::now() - begin_;
        (stats_->*complete)(duration, data_size);
        success_ = true;

        return duration;
    }

private:
    metricq::TimePoint begin_;
    DbStats* stats_;
    bool success_ = false;
};

using DbStatsReadTransaction = DbStatsTransaction<&DbStats::read_active, &DbStats::read_failed, &DbStats::read_complete>;
using DbStatsWriteTransaction = DbStatsTransaction<&DbStats::write_active, &DbStats::write_failed, &DbStats::write_complete>;