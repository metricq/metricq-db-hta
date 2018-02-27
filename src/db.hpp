#pragma once

#include <dataheap2/db.hpp>

#include <hta/hta.hpp>
#include <hta/directory.hpp>

#include <memory>

using json = nlohmann::json;

class Db : public dataheap2::Db
{
public:
    Db(const std::string& manager_host, const std::string& token="htaDb");

private:
    void sink_config_callback(const json& config) override;
    void ready_callback() override;
    void data_callback(const std::string& metric_name, dataheap2::TimeValue tv) override;

private:
    std::unique_ptr<hta::Directory> directory;
};
