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
#pragma once

#include <metricq/db.hpp>

#include <metricq/datachunk.pb.h>
#include <metricq/history.pb.h>

#include <hta/directory.hpp>
#include <hta/hta.hpp>

#include <asio/signal_set.hpp>

#include <memory>

using json = nlohmann::json;

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

class Db : public metricq::Db
{
public:
    Db(const std::string& manager_host, const std::string& token = "htaDb");

private:
    metricq::HistoryResponse on_history(const std::string& id,
                                        const metricq::HistoryRequest& content) override;
    void on_db_config(const json& config) override;
    void on_db_ready() override;
    void on_data(const std::string& metric_name, const metricq::DataChunk& chunk) override;

private:
    std::unique_ptr<hta::Directory> directory;
    asio::signal_set signals_;
};
