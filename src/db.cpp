#include "db.hpp"

Db::Db(const std::string& manager_host, const std::string& token) : dataheap2::Db(token)
{
    connect(manager_host);
}

void Db::sink_config_callback(const json& config)
{
    directory = std::make_unique<hta::Directory>(config);
}

void Db::ready_callback()
{
}

void Db::data_callback(const std::string& metric_name, dataheap2::TimeValue tv)
{
    (*directory)[metric_name]->insert(
        { hta::TimePoint(hta::duration_cast(tv.time.time_since_epoch())), tv.value });
}
