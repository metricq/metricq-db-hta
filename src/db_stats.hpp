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

    void read_pending();

    void read_active(metricq::Duration pending_duration);

    void read_complete(metricq::Duration active_duration, std::size_t data_size);

    void write_pending();

    void write_active(metricq::Duration pending_duration);

    void write_complete(metricq::Duration active_duration, std::size_t data_size);

    void collect();

private:
    class DbStatsImpl;

    std::unique_ptr<DbStatsImpl> impl;
};