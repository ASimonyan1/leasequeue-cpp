#include "leasequeue/queue.hpp"
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace leasequeue {
namespace {
std::uint64_t checksum(const std::string& text) {
    std::uint64_t result=14695981039346656037ULL;
    for(unsigned char c:text) { result^=c; result*=1099511628211ULL; }
    return result;
}
Tick add(Tick time, Tick duration) {
    if(time<0 || duration<0 || time>std::numeric_limits<Tick>::max()-duration)
        throw std::invalid_argument("Invalid time or duration overflow");
    return time+duration;
}
std::string hex(const std::string& input) {
    static constexpr char digits[]="0123456789abcdef";
    std::string out; out.reserve(input.size()*2);
    for(unsigned char c:input) { out+=digits[c>>4]; out+=digits[c&15]; }
    return out.empty() ? "-" : out;
}
std::string unhex(const std::string& input) {
    if(input=="-") return {};
    if(input.size()%2!=0) throw std::runtime_error("Bad journal hex");
    auto digit=[](char c) -> unsigned {
        if(c>='0' && c<='9') return static_cast<unsigned>(c-'0');
        if(c>='a' && c<='f') return static_cast<unsigned>(c-'a'+10);
        throw std::runtime_error("Bad journal hex");
    };
    std::string out;
    for(std::size_t i=0;i<input.size();i+=2) out+=static_cast<char>((digit(input[i])<<4)|digit(input[i+1]));
    return out;
}
}
FileJournal::FileJournal(std::filesystem::path path):path_(std::move(path)),lock_(path_.string()+".lock") {
    if(!std::filesystem::create_directory(lock_))
        throw std::runtime_error("Journal locked. After a crash verify no process owns it before removing .lock directory.");
}
FileJournal::~FileJournal() { std::error_code ignored; std::filesystem::remove(lock_,ignored); }
std::vector<std::string> FileJournal::replay() {
    if(!std::filesystem::exists(path_)) return {};
    std::ifstream file(path_,std::ios::binary);
    if(!file) throw std::runtime_error("Cannot open journal");
    std::vector<std::string> records;
    std::string line;
    std::uintmax_t complete_bytes=0;
    while(std::getline(file,line)) {
        if(file.eof()) break; // Last record without its final newline is uncommitted.
        auto separator=line.find(' ');
        if(separator==std::string::npos) throw std::runtime_error("Corrupt journal frame");
        std::uint64_t expected{};
        std::istringstream prefix(line.substr(0,separator));
        if(!(prefix>>std::hex>>expected) || !(prefix>>std::ws).eof()) throw std::runtime_error("Corrupt checksum");
        auto body=line.substr(separator+1);
        if(checksum(body)!=expected) throw std::runtime_error("Journal checksum mismatch");
        records.push_back(std::move(body));
        complete_bytes+=line.size()+1;
    }
    if(file.bad()) throw std::runtime_error("Journal read failed");
    file.close();
    if(std::filesystem::file_size(path_)!=complete_bytes) std::filesystem::resize_file(path_,complete_bytes);
    return records;
}
void FileJournal::append(const std::string& record) {
    std::ofstream file(path_,std::ios::binary|std::ios::app);
    if(!file) throw std::runtime_error("Cannot append journal");
    file<<std::hex<<checksum(record)<<' '<<record<<'\n';
    file.flush();
    if(!file) throw std::runtime_error("Journal flush failed");
    // OS page cache flush only: power-loss durability is deliberately not claimed.
}
Queue::Queue(std::shared_ptr<Journal> journal,std::size_t capacity):journal_(std::move(journal)),capacity_(capacity) {
    if(!journal_ || capacity_==0) throw std::invalid_argument("Journal and positive capacity required");
    for(const auto& record:journal_->replay()) {
        std::uint64_t seq{}; auto next=deserialize(record,seq);
        if(sequence_==std::numeric_limits<std::uint64_t>::max() || seq!=sequence_+1)
            throw std::runtime_error("Nonconsecutive journal sequence");
        jobs_=std::move(next); sequence_=seq;
    }
    if(jobs_.size()>capacity_) throw std::invalid_argument("Capacity smaller than restored state");
}
void Queue::ensure_healthy() const {
    if(poisoned_) throw std::runtime_error("Queue unavailable after journal failure; reopen and reconcile");
}
void Queue::commit(State next) {
    if(sequence_==std::numeric_limits<std::uint64_t>::max()) throw std::overflow_error("Sequence exhausted");
    auto record=serialize(next,sequence_+1);
    try { journal_->append(record); } catch(...) { poisoned_=true; throw; }
    jobs_.swap(next); ++sequence_;
}
std::uint64_t Queue::submit(std::string key,std::string payload,Tick now,unsigned max_attempts) {
    if(key.empty() || key.size()>128 || payload.size()>4096 || max_attempts==0 || max_attempts>100 || now<0)
        throw std::invalid_argument("Invalid job");
    std::lock_guard guard(mutex_); ensure_healthy();
    for(const auto& [id,job]:jobs_) {
        if(job.key==key) {
            if(job.payload!=payload || job.max_attempts!=max_attempts) throw std::invalid_argument("Idempotency key conflict");
            return id;
        }
    }
    if(jobs_.size()>=capacity_) throw std::length_error("Queue capacity reached (includes terminal jobs)");
    if(sequence_==std::numeric_limits<std::uint64_t>::max()) throw std::overflow_error("Sequence exhausted");
    auto next=jobs_; auto id=sequence_+1;
    next.emplace(id,Job{id,std::move(key),std::move(payload),Status::ready,0,max_attempts,now,0,0});
    commit(std::move(next)); return id;
}
std::optional<Lease> Queue::claim(Tick now,Tick lease_duration) {
    if(lease_duration<=0) throw std::invalid_argument("Lease duration must be positive");
    const Tick deadline=add(now,lease_duration);
    std::lock_guard guard(mutex_); ensure_healthy();
    auto next=jobs_; bool changed=false;
    for(auto& [id,job]:next) {
        (void)id;
        if(job.status==Status::leased && job.lease_until<=now) {
            job.status=job.attempts>=job.max_attempts ? Status::dead : Status::ready;
            job.available_at=now; changed=true;
        }
    }
    for(auto& [id,job]:next) {
        (void)id;
        if(job.status==Status::ready && job.available_at<=now) {
            if(sequence_==std::numeric_limits<std::uint64_t>::max()) throw std::overflow_error("Sequence exhausted");
            job.status=Status::leased; ++job.attempts; job.lease_until=deadline; job.token=sequence_+1;
            Lease result{job,job.token}; commit(std::move(next)); return result;
        }
    }
    if(changed) commit(std::move(next));
    return std::nullopt;
}
bool Queue::ack(std::uint64_t id,std::uint64_t token,Tick now) {
    if(now<0) throw std::invalid_argument("Negative time");
    std::lock_guard guard(mutex_); ensure_healthy(); auto found=jobs_.find(id);
    if(found==jobs_.end()) return false;
    const auto& job=found->second;
    if(job.status!=Status::leased || job.token!=token || now>=job.lease_until) return false;
    auto next=jobs_; next.at(id).status=Status::succeeded; commit(std::move(next)); return true;
}
bool Queue::nack(std::uint64_t id,std::uint64_t token,Tick now,Tick backoff) {
    Tick available=add(now,backoff);
    std::lock_guard guard(mutex_); ensure_healthy(); auto found=jobs_.find(id);
    if(found==jobs_.end()) return false;
    const auto& job=found->second;
    if(job.status!=Status::leased || job.token!=token || now>=job.lease_until) return false;
    auto next=jobs_; auto& changed=next.at(id);
    changed.status=changed.attempts>=changed.max_attempts ? Status::dead : Status::ready;
    changed.available_at=available; commit(std::move(next)); return true;
}
std::optional<Job> Queue::get(std::uint64_t id) const {
    std::lock_guard guard(mutex_); ensure_healthy();
    auto found=jobs_.find(id); return found==jobs_.end() ? std::nullopt : std::optional<Job>(found->second);
}
Stats Queue::stats() const {
    std::lock_guard guard(mutex_); ensure_healthy(); Stats result;
    for(const auto& [id,job]:jobs_) {
        (void)id;
        switch(job.status) {
            case Status::ready: ++result.ready; break;
            case Status::leased: ++result.leased; break;
            case Status::succeeded: ++result.succeeded; break;
            case Status::dead: ++result.dead; break;
        }
    }
    return result;
}
std::string Queue::serialize(const State& jobs,std::uint64_t sequence) {
    std::ostringstream out; out<<"LQ1 "<<sequence<<' '<<jobs.size();
    for(const auto& [id,j]:jobs)
        out<<' '<<id<<' '<<hex(j.key)<<' '<<hex(j.payload)<<' '<<static_cast<int>(j.status)
           <<' '<<j.attempts<<' '<<j.max_attempts<<' '<<j.available_at<<' '<<j.lease_until<<' '<<j.token;
    return out.str();
}
Queue::State Queue::deserialize(const std::string& text,std::uint64_t& sequence) {
    std::istringstream in(text); std::string magic; std::size_t count{};
    if(!(in>>magic>>sequence>>count) || magic!="LQ1" || count>1000000) throw std::runtime_error("Invalid snapshot header");
    State state; std::map<std::string,bool> keys;
    for(std::size_t i=0;i<count;++i) {
        Job j; int status{}; std::string key,payload;
        if(!(in>>j.id>>key>>payload>>status>>j.attempts>>j.max_attempts>>j.available_at>>j.lease_until>>j.token))
            throw std::runtime_error("Invalid snapshot entry");
        j.key=unhex(key); j.payload=unhex(payload);
        if(status<0 || status>3 || j.id==0 || j.id>sequence || j.token>sequence || j.max_attempts==0 || j.max_attempts>100
            || j.attempts>j.max_attempts || j.available_at<0 || j.lease_until<0 || j.key.empty() || j.key.size()>128 || j.payload.size()>4096)
            throw std::runtime_error("Invalid snapshot values");
        j.status=static_cast<Status>(status);
        if(!keys.emplace(j.key,true).second || !state.emplace(j.id,std::move(j)).second) throw std::runtime_error("Duplicate snapshot entry");
    }
    if(!(in>>std::ws).eof()) throw std::runtime_error("Trailing snapshot data");
    return state;
}
}
