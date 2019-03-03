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
: metricq::Db(token), signals_(io_service, SIGINT, SIGTERM)
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
}

void Db::on_closed()
{
    Log::debug() << "Connection to MetricQ closed.";
    signals_.cancel();
}

void Db::on_db_config(const json& config)
{
    // TODO make shared self voodoo to avoid disaster
    // TODO use strand instead generic io_service of executor
    auto config_complete_handler = asio::bind_executor(io_service.get_executor(), [this]() {
        Log::debug() << "config_completion_handler()";
        setup_data_queue();
        setup_history_queue();
    });
    Log::debug() << "on_db_config";
    async_hta.async_config(config, config_complete_handler);
}

void Db::on_db_ready()
{
}

bool Db::on_data(const std::string& metric_name, const metricq::DataChunk& chunk,
                 uint64_t delivery_tag)
{
    Log::trace() << "data_callback with " << chunk.value_size() << " values";

    // TODO make shared self voodoo to avoid disaster
    // TODO use strand instead generic io_service of executor
    auto confirm_handler = asio::bind_executor(
        io_service.get_executor(), [this, delivery_tag]() { data_confirm(delivery_tag); });
    async_hta.async_write(metric_name, chunk, confirm_handler);

    return false;
}

void Db::on_history(const std::string& id, const metricq::HistoryRequest& content,
                    std::function<void(const metricq::HistoryResponse&)>& respond)
{
    // TODO make shared self voodoo to avoid disaster
    // TODO use strand instead generic io_service of executor
    auto confirm_handler = asio::bind_executor(
        io_service.get_executor(),
        [this, respond = std::move(respond)](metricq::HistoryResponse response) {
            respond(std::move(response));
        });
    async_hta.async_read(id, content, confirm_handler);
}
