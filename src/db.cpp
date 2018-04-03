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
    assert(directory);
    auto metric = (*directory)[metric_name];
    for (auto tv : chunk)
    {
        metric->insert(TimeValue(tv));
    }
    metric->flush();
}

dataheap2::HistoryResponse Db::history_callback(const std::string& id,
                                                const dataheap2::HistoryRequest& content)
{
    std::cerr << "History call\n";
    dataheap2::HistoryResponse response;
    response.set_metric(id);

    auto metric = (*directory)[id];

    auto start_ts = content.start_time();
    auto end_ts = content.end_time();

    // std::cerr << "Start " << std::ctime(&start_ts) << " End " << std::ctime(&end_ts) << "\n";

    hta::TimePoint start_time(hta::duration_cast(std::chrono::nanoseconds(content.start_time())));
    hta::TimePoint end_time(hta::duration_cast(std::chrono::nanoseconds(content.end_time())));
    auto interval_ns = hta::duration_cast(std::chrono::nanoseconds(content.interval_ns()));

    std::cerr << "Start " << start_time << " end " << end_time << " intervl " << interval_ns.count()
              << "\n";

    auto rows = metric->retrieve(start_time, end_time, interval_ns);
    std::cerr << "Rows count " << rows.size() << " raw " << metric->count(start_time, end_time)
              << "\n";
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
