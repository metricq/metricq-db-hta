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

#include "async_hta_service.hpp"
#include "db_stats.hpp"

#include <metricq/db.hpp>
#include <metricq/json.hpp>
#include <metricq/timer.hpp>

#include <metricq/datachunk.pb.h>
#include <metricq/history.pb.h>

#include <asio/signal_set.hpp>

#include <memory>

class Db : public metricq::Db
{
public:
    Db(const std::string& manager_host, const std::string& token = "metricq-db-hta");

protected:
    void on_db_config(const metricq::json& config, metricq::Db::ConfigCompletion complete) override;

    void on_history(const std::string& id, const metricq::HistoryRequest& content,
                    metricq::Db::HistoryCompletion complete) override;

    void on_db_ready() override;

    void on_data(const std::string& metric_name, const metricq::DataChunk& chunk,
                 metricq::Db::DataCompletion complete) override;

protected:
    void on_error(const std::string& message) override;

    void on_closed() override;

private:
    AsyncHtaService async_hta;
    asio::signal_set signals_;
    metricq::Timer stats_timer_;
};
