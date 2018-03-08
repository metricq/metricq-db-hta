#include "db.hpp"

#include "log.hpp"

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

void Db::sink_config_callback(const json& config)
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
