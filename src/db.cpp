#include "db.hpp"

#include "log.hpp"

#include <hta/ostream.hpp>

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

void Db::db_config_callback(const json& config)
{
    directory = std::make_unique<hta::Directory>(config);
}

void Db::ready_callback()
{
    assert(directory);
}

void Db::data_callback(const std::string& metric_name, const metricq::DataChunk& chunk)
{
    Log::trace() << "data_callback with " << chunk.value_size() << " values";
    assert(directory);
    auto metric = (*directory)[metric_name];
    auto range = metric->range();
    uint64_t skip = 0;
    for (TimeValue tv : chunk)
    {
        if (tv.htv.time < range.second)
        {
            skip++;
            continue;
        }
        metric->insert(tv);
    }
    if (skip > 0)
    {
        Log::error() << "Skipped " << skip << " non-monotonic of " << chunk.value_size()
                     << " values";
    }
    Log::trace() << "data_callback complete";
    metric->flush();
}

metricq::HistoryResponse Db::history_callback(const std::string& id,
                                                const metricq::HistoryRequest& content)
{
    metricq::HistoryResponse response;
    response.set_metric(id);

    Log::debug() << "history_callback get metric";
    auto metric = (*directory)[id];

    hta::TimePoint start_time(hta::duration_cast(std::chrono::nanoseconds(content.start_time())));
    hta::TimePoint end_time(hta::duration_cast(std::chrono::nanoseconds(content.end_time())));
    auto interval_ns = hta::duration_cast(std::chrono::nanoseconds(content.interval_ns()));

    Log::debug() << "history_callback get data";
    auto rows = metric->retrieve(start_time, end_time, interval_ns);
    Log::debug() << "history_callback got data";

    hta::TimePoint last_time;

    Log::debug() << "history_callback build response";
    for (auto row : rows)
    {
        auto time_delta =
            std::chrono::duration_cast<std::chrono::nanoseconds>(row.time - last_time);
        response.add_time_delta(time_delta.count());
        response.add_value_min(row.aggregate.minimum);
        response.add_value_max(row.aggregate.maximum);
        response.add_value_avg(row.aggregate.mean());
        last_time = row.time;
    }
    Log::debug() << "history_callback build response done";

    return response;
}
