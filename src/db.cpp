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
#include "db.hpp"

#include "log.hpp"

#include <hta/ostream.hpp>

#include <asio.hpp>

#include <chrono>
#include <ratio>

Db::Db(const std::string& manager_host, const std::string& token)
: metricq::Db(token), signals_(io_service, SIGINT, SIGTERM), stats_timer_(io_service)
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

void Db::on_error(const std::string& message)
{
    Log::error() << "Connection to MetricQ failed: " << message;
    signals_.cancel();
    stats_timer_.cancel();
}

void Db::on_closed()
{
    Log::debug() << "Connection to MetricQ closed.";
    signals_.cancel();
    stats_timer_.cancel();
}

void Db::on_db_config(const metricq::json& config, metricq::Db::ConfigCompletion complete)
{
    Log::debug() << "on_db_config";
    if (config.count("stats"))
    {
        auto stats = config.at("stats");
        auto prefix = stats.at("prefix").get<std::string>();
        auto rate = stats.at("rate").get<double>();
        if (rate <= 0)
        {
            throw std::runtime_error("invalid rate configured for stats");
        }
        async_hta.stats().init(*this, prefix, rate);
        stats_interval_ = metricq::duration_cast(std::chrono::duration<double>(1 / rate));
        if (stats_interval_.count() <= 0)
        {
            throw std::runtime_error("rate for stats results in invalid interval");
        }
    }
    async_hta.async_config(config, std::move(complete));
}

void Db::on_db_ready()
{
    // the initial collect should be empty, and we want that
    if (stats_interval_.count())
    {
        async_hta.stats().collect();
        stats_timer_.start(
            [this](auto) {
                async_hta.stats().collect();
                return metricq::Timer::TimerResult::repeat;
            },
            stats_interval_);
    }
}

void Db::on_data(const std::string& metric_name, const metricq::DataChunk& chunk,
                 metricq::Db::DataCompletion complete)
{
    Log::trace() << "data_callback with " << chunk.value_size() << " values";

    async_hta.async_write(metric_name, chunk, std::move(complete));
}

void Db::on_history(const std::string& id, const metricq::HistoryRequest& content,
                    metricq::Db::HistoryCompletion complete)
{
    async_hta.async_read(id, content, std::move(complete));
}
