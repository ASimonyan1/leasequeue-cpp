#pragma once
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace leasequeue {
using Tick = std::int64_t;
enum class Status { ready, leased, succeeded, dead };
struct Job {
    std::uint64_t id{};
    std::string key;
    std::string payload;
    Status status{Status::ready};
    unsigned attempts{};
    unsigned max_attempts{3};
    Tick available_at{};
    Tick lease_until{};
    std::uint64_t token{};
};
struct Lease { Job job; std::uint64_t token; };
struct Stats { std::size_t ready{}, leased{}, succeeded{}, dead{}; };

// append must either persist the complete record or throw. A failed Queue is
// poisoned until reopened, because an I/O failure can have an ambiguous outcome.
class Journal {
public:
    virtual ~Journal() = default;
    virtual std::vector<std::string> replay() = 0;
    virtual void append(const std::string& record) = 0;
};
class FileJournal final : public Journal {
public:
    explicit FileJournal(std::filesystem::path path);
    ~FileJournal() override;
    FileJournal(const FileJournal&) = delete;
    FileJournal& operator=(const FileJournal&) = delete;
    std::vector<std::string> replay() override;
    void append(const std::string& record) override;
private:
    std::filesystem::path path_, lock_;
};

class Queue {
public:
    explicit Queue(std::shared_ptr<Journal> journal, std::size_t capacity=10000);
    std::uint64_t submit(std::string key, std::string payload, Tick now, unsigned max_attempts=3);
    std::optional<Lease> claim(Tick now, Tick lease_duration);
    bool ack(std::uint64_t id, std::uint64_t token, Tick now);
    bool nack(std::uint64_t id, std::uint64_t token, Tick now, Tick backoff);
    std::optional<Job> get(std::uint64_t id) const;
    Stats stats() const;
private:
    using State = std::map<std::uint64_t, Job>;
    mutable std::mutex mutex_;
    std::shared_ptr<Journal> journal_;
    State jobs_;
    std::uint64_t sequence_{};
    std::size_t capacity_;
    bool poisoned_{};
    void ensure_healthy() const;
    void commit(State next);
    static std::string serialize(const State& jobs, std::uint64_t sequence);
    static State deserialize(const std::string& text, std::uint64_t& sequence);
};
}
