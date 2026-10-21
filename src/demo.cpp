#include "leasequeue/queue.hpp"
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

int main(int argc,char** argv) {
    try {
        auto journal=std::make_shared<leasequeue::FileJournal>(argc>1 ? argv[1] : "demo.wal");
        leasequeue::Queue queue(journal);
        auto now=[] { return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count(); };
        for(int i=0;i<20;++i) queue.submit("demo-"+std::to_string(i),"Send reminder "+std::to_string(i),now());
        std::vector<std::jthread> workers;
        for(int i=0;i<4;++i) workers.emplace_back([&] {
            while(auto lease=queue.claim(now(),30000)) {
                // External side effects must themselves deduplicate by job ID.
                queue.ack(lease->job.id,lease->token,now());
            }
        });
        workers.clear(); // Join before reading final statistics.
        auto stats=queue.stats();
        std::cout<<"succeeded="<<stats.succeeded<<" ready="<<stats.ready<<" leased="<<stats.leased<<" dead="<<stats.dead<<'\n';
    } catch(const std::exception& error) { std::cerr<<error.what()<<'\n'; return 1; }
}
