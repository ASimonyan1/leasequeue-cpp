#include "leasequeue/queue.hpp"
#include <atomic>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace leasequeue;
namespace {
void require(bool condition,const char* message) { if(!condition) throw std::runtime_error(message); }
template<class F> void throws(F action) { bool failed=false; try { action(); } catch(const std::exception&) { failed=true; } require(failed,"Expected exception"); }
struct MemoryJournal:Journal {
    std::vector<std::string> records;
    bool fail=false;
    std::vector<std::string> replay() override { return records; }
    void append(const std::string& record) override {
        if(fail) throw std::runtime_error("Injected I/O failure");
        records.push_back(record);
    }
};
void idempotency() {
    auto log=std::make_shared<MemoryJournal>(); Queue q(log,1);
    auto id=q.submit("key","payload",0);
    require(q.submit("key","payload",5)==id,"Duplicate key changed ID");
    require(log->records.size()==1,"Replay appended event");
    throws([&]{q.submit("key","different",0);});
    throws([&]{q.submit("another","",0);});
    throws([&]{q.submit("","",0);});
}
void lease_fencing() {
    auto log=std::make_shared<MemoryJournal>(); Queue q(log);
    auto id=q.submit("key","payload",0);
    auto a=q.claim(0,10).value();
    require(!q.claim(9,10),"Claimed before expiry");
    require(!q.ack(id,a.token,10),"Accepted at exact expiry");
    auto b=q.claim(10,10).value();
    require(a.token!=b.token,"Lease token reused");
    require(!q.ack(id,a.token,11),"Accepted stale token");
    require(!q.nack(id,a.token,11,0),"Retried stale token");
    require(q.ack(id,b.token,11),"Fresh ack failed");
    require(!q.ack(id,b.token,12),"Duplicate ack accepted");
    require(q.stats().succeeded==1,"Success not counted");
}
void retries_and_dead_letter() {
    auto log=std::make_shared<MemoryJournal>(); Queue q(log);
    auto id=q.submit("retry","",0,2);
    auto a=q.claim(0,10).value(); require(q.nack(id,a.token,1,10),"nack failed");
    require(!q.claim(10,5),"Backoff ignored");
    auto b=q.claim(11,5).value(); require(b.job.attempts==2,"Attempts not incremented");
    require(!q.claim(16,5),"Exhausted job reclaimed");
    require(q.stats().dead==1,"Expired final lease not dead");
}
void restart_preserves_tokens() {
    auto log=std::make_shared<MemoryJournal>();
    std::uint64_t id{},token{};
    { Queue q(log); id=q.submit("binary",std::string("a\n\0b",4),0); token=q.claim(0,10)->token; }
    Queue restored(log);
    require(restored.get(id)->payload==std::string("a\n\0b",4),"Payload changed in replay");
    require(!restored.claim(9,10),"Restart lost lease");
    auto b=restored.claim(10,10).value();
    require(b.token>token,"Restart reused token");
    require(!restored.ack(id,token,11),"Stale ack after restart");
    require(restored.ack(id,b.token,11),"Fresh ack after restart failed");
}
void journal_failure_is_atomic() {
    auto log=std::make_shared<MemoryJournal>(); Queue q(log);
    auto id=q.submit("job","",0); auto lease=q.claim(0,10).value();
    log->fail=true; throws([&]{q.ack(id,lease.token,1);});
    throws([&]{q.stats();}); throws([&]{q.submit("other","",1);});
    log->fail=false; Queue reopened(log);
    require(reopened.get(id)->status==Status::leased,"Failed commit partially applied");
    require(reopened.ack(id,lease.token,2),"Recovery failed");
}
void concurrent_workers() {
    auto log=std::make_shared<MemoryJournal>(); Queue q(log);
    for(int i=0;i<150;++i) q.submit(std::to_string(i),"",0);
    std::atomic<int> acked{0}; std::atomic<bool> failed{false};
    std::vector<std::jthread> threads;
    for(int i=0;i<8;++i) threads.emplace_back([&]{
        try { while(auto lease=q.claim(0,1000)) { if(q.ack(lease->job.id,lease->token,1)) ++acked; else failed=true; } }
        catch(...) { failed=true; }
    });
    threads.clear(); require(!failed && acked==150,"Concurrent processing lost or duplicated jobs");
    require(q.stats().succeeded==150,"Concurrent final stats wrong");
}
void concurrent_submit_deduplicates() {
    auto log=std::make_shared<MemoryJournal>(); Queue q(log);
    std::vector<std::jthread> threads; std::atomic<bool> failed{false};
    for(int i=0;i<8;++i) threads.emplace_back([&]{try { for(int j=0;j<50;++j) q.submit("same","body",0); } catch(...) {failed=true;} });
    threads.clear(); require(!failed && q.stats().ready==1 && log->records.size()==1,"Concurrent dedupe failed");
}
void invalid_times() {
    auto log=std::make_shared<MemoryJournal>(); Queue q(log);
    throws([&]{q.claim(0,0);}); throws([&]{q.claim(-1,10);});
    throws([&]{q.claim(std::numeric_limits<Tick>::max(),1);});
    throws([&]{q.submit("key","",-1);});
    require(!q.ack(999,1,0),"Unknown ack accepted");
}
void file_recovery() {
    auto path=std::filesystem::temp_directory_path()/"leasequeue-contract-test.wal";
    std::filesystem::remove(path);
    {
        auto log=std::make_shared<FileJournal>(path); Queue q(log); q.submit("one","",0);
        throws([&]{FileJournal other(path);});
    }
    auto original_size=std::filesystem::file_size(path);
    { std::ofstream file(path,std::ios::app|std::ios::binary); file<<"torn incomplete tail"; }
    { Queue q(std::make_shared<FileJournal>(path)); require(q.stats().ready==1,"Lost committed state"); }
    require(std::filesystem::file_size(path)==original_size,"Torn tail not truncated");
    { std::ofstream file(path,std::ios::app|std::ios::binary); file<<"0 corrupt\n"; }
    throws([&]{Queue q(std::make_shared<FileJournal>(path));});
    std::filesystem::remove(path);
}
void replay_sequence_validation() {
    auto log=std::make_shared<MemoryJournal>(); { Queue q(log); q.submit("one","",0); }
    log->records.push_back(log->records.back()); throws([&]{Queue q(log);});
}
}
int main() {
    const std::vector<std::pair<const char*,void(*)()>> tests={
        {"idempotency",idempotency},{"lease fencing",lease_fencing},{"retry budget",retries_and_dead_letter},
        {"restart",restart_preserves_tokens},{"I/O rollback",journal_failure_is_atomic},{"workers",concurrent_workers},
        {"concurrent submit",concurrent_submit_deduplicates},{"invalid time",invalid_times},
        {"file recovery",file_recovery},{"sequence validation",replay_sequence_validation}};
    try { for(auto [name,test]:tests) {test(); std::cout<<"PASS "<<name<<'\n';} }
    catch(const std::exception& e) {std::cerr<<"FAIL "<<e.what()<<'\n'; return 1;}
}
