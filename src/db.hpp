#pragma once

#include <dataheap2/db.hpp>

#include <dataheap2/datachunk.pb.h>
#include <dataheap2/history.pb.h>

#include <hta/directory.hpp>
#include <hta/hta.hpp>

#include <asio/signal_set.hpp>

#include <memory>

using json = nlohmann::json;

struct TimeValue
{
    TimeValue(dataheap2::TimeValue dtv)
    : htv{ hta::TimePoint(hta::duration_cast(dtv.time.time_since_epoch())), dtv.value }
    {
    }

    operator hta::TimeValue() const
    {
        return htv;
    }

    hta::TimeValue htv;
};

class Db : public dataheap2::Db
{
public:
    Db(const std::string& manager_host, const std::string& token = "htaDb");
private:
    dataheap2::HistoryResponse history_callback(const std::string& id,
                                                const dataheap2::HistoryRequest& content) override;
    void db_config_callback(const json& config) override;
    void ready_callback();
    void data_callback(const std::string& metric_name, const dataheap2::DataChunk& chunk) override;

private:
    std::unique_ptr<hta::Directory> directory;
    asio::signal_set signals_;
};
