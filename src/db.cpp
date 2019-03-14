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

void Db::on_db_config(const metricq::json& config, metricq::Db::ConfigCompletion complete)
{
    Log::debug() << "on_db_config";
    async_hta.async_config(config, std::move(complete));
}

void Db::on_db_ready()
{
}

void Db::on_data(const std::string& metric_name, const metricq::DataChunk& chunk,
                 metricq::Db::DataCompletion complete)
{
    if (!received_chunk_)
    {
        received_chunk_ = true;
        Log::info() << "received first datachunk "
                    << metricq::Clock::now().time_since_epoch().count();
    }
    Log::trace() << "data_callback with " << chunk.value_size() << " values";

    async_hta.async_write(metric_name, chunk, std::move(complete));
}

void Db::on_data(const AMQP::Message& message, uint64_t delivery_tag, bool redelivered)
{
    if (message.typeName() == "end")
    {
        data_channel_->ack(delivery_tag);
        Log::info() << "received end message, requesting release and stop";
        // We used to close the data connection here, but this should not be necessary.
        // It will be closed implicitly from the response callback.
        rpc("sink.release", [this](const auto&) { close(); }, { { "dataQueue", data_queue_ } });
        return;
    }

    Sink::on_data(message, delivery_tag, redelivered);
}

void Db::on_history(const std::string& id, const metricq::HistoryRequest& content,
                    metricq::Db::HistoryCompletion complete)
{
    async_hta.async_read(id, content, std::move(complete));
}
