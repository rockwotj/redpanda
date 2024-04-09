/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "ai/service.h"
#include "base/vassert.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>

#include <absl/strings/str_split.h>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>

namespace ai {

float embd_similarity_cos(const float* embd1, const float* embd2, int n) {
    double sum = 0.0;
    double sum1 = 0.0;
    double sum2 = 0.0;

    for (int i = 0; i < n; i++) {
        sum += embd1[i] * embd2[i];
        sum1 += embd1[i] * embd1[i];
        sum2 += embd2[i] * embd2[i];
    }

    return sum / (sqrt(sum1) * sqrt(sum2));
}
ss::future<> run(const boost::program_options::variables_map& cfg) {
    ss::sharded<service> s;
    co_await s.start();
    service::config service_config{
      .model_file = cfg["model_file"].as<std::filesystem::path>(),
    };
    auto question = cfg["question"].as<std::string>();
    co_await s.invoke_on_all(
      [&service_config](service& s) { return s.start(service_config); });
    auto qe = co_await s.local().compute_embeddings(question);
    std::string line;
    std::string contents;
    while (std::getline(std::cin, line)) {
        contents += line;
        contents += " ";
    }
    std::cerr << "article: " << contents << std::endl << std::endl;
    float highest = -1.0;
    for (const auto& sentence :
         absl::StrSplit(contents, ".", absl::SkipEmpty())) {
        auto response = co_await s.local().compute_embeddings(
          ss::sstring(sentence));
        float sim = embd_similarity_cos(
          qe.data(), response.data(), int(response.size()));
        std::cerr << "sentence: " << sentence << std::endl << std::endl;
        std::cerr << "simularity: " << sim << std::endl;
        highest = std::max(sim, highest);
    }
    std::cout << "max: " << highest << std::endl;
    co_await s.stop();
}
} // namespace ai

int main(int argc, char** argv) {
    ss::app_template app;

    namespace po = boost::program_options;
    app.add_options()(
      "model_file",
      po::value<std::filesystem::path>(),
      "The path to the ML model");

    app.add_options()("question", po::value<std::string>(), "question to ask");

    return app.run(argc, argv, [&] {
        const boost::program_options::variables_map& cfg = app.configuration();
        return ai::run(cfg).then([] { return 0; });
    });
}
