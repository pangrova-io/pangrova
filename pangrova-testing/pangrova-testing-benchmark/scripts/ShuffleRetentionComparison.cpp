#include "engine/shuffle/ShuffleBufferStore.h"
#include <chrono>
#include <future>
#include <iostream>
#include <thread>

int main() {
  using namespace pangrova::shuffle;
  using namespace std::chrono;
  std::cout << "iteration,store_status,producer_call_ns,retained_records,"
               "retained_spill_bytes,retained_files,replay_bytes,final_records,"
               "final_spill_bytes,final_files\n";
  for (int i = 0; i < 12; ++i) {
    ShuffleBufferLimits limits;
    limits.memory_bytes = 0;
    limits.buffered_batches = 1;
    limits.max_payload_bytes = 65536;
    auto store = std::make_shared<ShuffleBufferStore>(limits);
#if PER_HANDLE_RETENTION
    ShuffleBufferStore::HandleScope scope(store);
    auto put = [&](ShuffleBatch batch) {
      return store->store(std::move(batch), scope);
    };
#else
    auto put = [&](ShuffleBatch batch) {
      return store->store(std::move(batch));
    };
#endif
    std::string data(65536, 'y');
    auto first =
        put(ShuffleBatch{0, folly::IOBuf::copyBuffer(data), 16}).ValueOrDie();
    std::promise<void> entering;
    auto started = entering.get_future();
    auto waiting = std::async(std::launch::async, [&] {
      auto batch = ShuffleBatch{0, folly::IOBuf::copyBuffer(data), 16};
      auto t = steady_clock::now();
      entering.set_value();
      auto result = put(std::move(batch));
      auto elapsed =
          duration_cast<nanoseconds>(steady_clock::now() - t).count();
      return std::make_pair(std::move(result), elapsed);
    });
    started.get();
    std::this_thread::sleep_for(milliseconds(40));
    auto retained = store->usage();
    first.reset();
    auto [second, elapsed] = waiting.get();
    size_t replayed = 0;
    if (second.ok()) {
      auto batch = store->replay(**second).ValueOrDie();
      if (batch.data->to<std::string>() != data)
        return 1;
      replayed = batch.data->computeChainDataLength();
      second.ValueOrDie().reset();
    } else if (!second.status().IsCapacityError())
      return 2;
    auto final = store->usage();
    if (retained.buffered_batches != 1 || final.buffered_batches ||
        final.spill_bytes || final.spill_files)
      return 3;
    std::cout << i << ',' << (second.ok() ? "OK" : "CAPACITY_ERROR") << ','
              << elapsed << ',' << retained.buffered_batches << ','
              << retained.spill_bytes << ',' << retained.spill_files << ','
              << replayed << ',' << final.buffered_batches << ','
              << final.spill_bytes << ',' << final.spill_files << '\n';
  }
}
