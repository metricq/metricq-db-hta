#include "db.hpp"

#include "log.hpp"

#include <hta/ostream.hpp>

Db::Db(const std::string& manager_host, const std::string& token)
: dataheap2::Db(token), signals_(io_service, SIGINT, SIGTERM)
{
    signals_.async_wait([this](auto, auto signal) {
        if (!signal)
        {
            return;
        }
        Log::info() << "Caught signal " << signal << ". Shutdown dataheap2-db-hta.";
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

void Db::data_callback(const std::string& metric_name, const dataheap2::DataChunk& chunk)
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
        Log::error() << "Skipped " << skip << " non-monotonic of " << chunk.value_size() << " values";
    }
    Log::trace() << "data_callback complete";
    metric->flush();
}

dataheap2::HistoryResponse Db::history_callback(const std::string& id,
                                                const dataheap2::HistoryRequest& content)
{
    dataheap2::HistoryResponse response;
    response.set_metric(id);

    auto metric = (*directory)[id];

    hta::TimePoint start_time(hta::duration_cast(std::chrono::nanoseconds(content.start_time())));
    hta::TimePoint end_time(hta::duration_cast(std::chrono::nanoseconds(content.end_time())));
    auto interval_ns = hta::duration_cast(std::chrono::nanoseconds(content.interval_ns()));

    auto rows = metric->retrieve(start_time, end_time, interval_ns);

    hta::TimePoint last_time;

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

    return response;
}
