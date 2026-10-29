#include "leasequeue/queue.hpp"
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <thread>

using json=nlohmann::json;
namespace {
volatile std::sig_atomic_t interrupted=0;
void on_signal(int) { interrupted=1; }
std::string env(const char* key,std::string fallback={}) {
    const char* value=std::getenv(key); return value?value:std::move(fallback);
}
leasequeue::Tick now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
void reply(httplib::Response& res,int status,const json& value) {
    res.status=status; res.set_content(value.dump(),"application/json");
}
json parse(const httplib::Request& req,std::initializer_list<const char*> fields) {
    auto j=json::parse(req.body);
    if(!j.is_object() || j.size()!=fields.size()) throw std::invalid_argument("invalid_fields");
    for(auto field:fields) if(!j.contains(field)) throw std::invalid_argument("invalid_fields");
    return j;
}
std::uint64_t integer(const json& j,const char* key,std::uint64_t limit) {
    const auto& n=j.at(key);
    if(!n.is_number_integer() || n<0 || n>limit) throw std::invalid_argument("invalid_integer");
    return n.get<std::uint64_t>();
}
std::string text(const json& j,const char* key) {
    if(!j.at(key).is_string()) throw std::invalid_argument("invalid_string");
    return j.at(key).get<std::string>();
}
std::uint64_t identifier(const std::string& s) {
    std::uint64_t n{}; const auto parsed=std::from_chars(s.data(),s.data()+s.size(),n);
    if(parsed.ec!=std::errc{} || parsed.ptr!=s.data()+s.size() || n==0) throw std::invalid_argument("invalid_id");
    return n;
}
json job_json(const leasequeue::Job& job) {
    const char* status="ready";
    switch(job.status) {
        case leasequeue::Status::ready: break;
        case leasequeue::Status::leased: status="leased"; break;
        case leasequeue::Status::succeeded: status="succeeded"; break;
        case leasequeue::Status::dead: status="dead"; break;
    }
    return {{"id",job.id},{"key",job.key},{"payload",job.payload},{"status",status},
            {"attempts",job.attempts},{"max_attempts",job.max_attempts},{"lease_until",job.lease_until},{"token",job.token}};
}
template<class F> auto guarded(F f) {
    return [f](const httplib::Request& req,httplib::Response& res) {
        try { f(req,res); }
        catch(const json::exception&) { reply(res,400,{{"error","invalid_json"}}); }
        catch(const std::invalid_argument& e) {
            const bool conflict=std::string(e.what())=="Idempotency key conflict";
            reply(res,conflict?409:400,{{"error",conflict?"idempotency_conflict":"invalid_request"}});
        }
        catch(const std::exception&) { reply(res,503,{{"error","queue_unavailable"}}); }
    };
}
}
int main() {
    try {
        const auto token=env("API_TOKEN");
        if(token.size()<16) throw std::runtime_error("API_TOKEN must contain at least 16 characters");
        auto journal=std::make_shared<leasequeue::FileJournal>(env("JOURNAL_PATH","queue.wal"));
        leasequeue::Queue queue(journal);
        httplib::Server server;
        server.new_task_queue=[] { return new httplib::ThreadPool(6,6,32); };
        server.set_read_timeout(2); server.set_write_timeout(2);
        server.set_payload_max_length(8192); server.set_keep_alive_max_count(20);
        server.set_pre_routing_handler([&](const auto& req,auto& res) {
            if(req.path!="/health" && req.get_header_value("Authorization")!="Bearer "+token) {
                reply(res,401,{{"error","unauthorized"}}); return httplib::Server::HandlerResponse::Handled;
            }
            return httplib::Server::HandlerResponse::Unhandled;
        });
        server.Get("/health",guarded([&](const auto&,auto& res) {
            (void)queue.stats(); reply(res,200,{{"ready",true}});
        }));
        server.Get("/v1/stats",guarded([&](const auto&,auto& res) {
            const auto s=queue.stats(); reply(res,200,{{"ready",s.ready},{"leased",s.leased},{"succeeded",s.succeeded},{"dead",s.dead}});
        }));
        server.Post("/v1/jobs",guarded([&](const auto& req,auto& res) {
            const auto j=parse(req,{"key","payload","max_attempts"});
            const auto attempts=integer(j,"max_attempts",100);
            if(attempts==0) throw std::invalid_argument("invalid_attempts");
            const auto id=queue.submit(text(j,"key"),text(j,"payload"),now(),static_cast<unsigned>(attempts));
            reply(res,200,{{"id",id}}); // New and replayed submissions share the same stable ID.
        }));
        server.Get(R"(/v1/jobs/(\d+))",guarded([&](const auto& req,auto& res) {
            const auto job=queue.get(identifier(req.matches[1]));
            if(!job) { reply(res,404,{{"error","job_not_found"}}); return; }
            reply(res,200,job_json(*job));
        }));
        server.Post("/v1/claims",guarded([&](const auto& req,auto& res) {
            const auto j=parse(req,{"lease_ms"}); const auto duration=integer(j,"lease_ms",60000);
            if(duration==0) throw std::invalid_argument("invalid_duration");
            const auto lease=queue.claim(now(),static_cast<leasequeue::Tick>(duration));
            if(!lease) { res.status=204; return; }
            reply(res,201,job_json(lease->job));
        }));
        server.Post(R"(/v1/jobs/(\d+)/ack)",guarded([&](const auto& req,auto& res) {
            const auto j=parse(req,{"token"});
            const bool ok=queue.ack(identifier(req.matches[1]),integer(j,"token",std::numeric_limits<std::uint64_t>::max()),now());
            reply(res,ok?200:409,{{"accepted",ok}});
        }));
        server.Post(R"(/v1/jobs/(\d+)/nack)",guarded([&](const auto& req,auto& res) {
            const auto j=parse(req,{"token","backoff_ms"});
            const bool ok=queue.nack(identifier(req.matches[1]),integer(j,"token",std::numeric_limits<std::uint64_t>::max()),now(),
                                    static_cast<leasequeue::Tick>(integer(j,"backoff_ms",60000)));
            reply(res,ok?200:409,{{"accepted",ok}});
        }));
        server.set_error_handler([](const auto&,auto& res) { if(res.body.empty()) reply(res,res.status,{{"error","http_error"}}); });
        std::signal(SIGINT,on_signal); std::signal(SIGTERM,on_signal);
        const int port=std::stoi(env("PORT","8081"));
        if(port<1 || port>65535) throw std::runtime_error("Invalid PORT");
        if(!server.bind_to_port(env("HOST","127.0.0.1"),port)) throw std::runtime_error("Cannot bind HTTP port");
        std::jthread watcher([&](std::stop_token stop) {
            while(!stop.stop_requested() && !interrupted) std::this_thread::sleep_for(std::chrono::milliseconds(50));
            if(interrupted) server.stop();
        });
        std::cout<<"queue listening on port "<<port<<std::endl;
        const bool ok=server.listen_after_bind(); watcher.request_stop(); return ok?0:1;
    } catch(const std::exception& e) { std::cerr<<e.what()<<'\n'; return 1; }
}
