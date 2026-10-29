FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ cmake make ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --parallel 2
FROM debian:bookworm-slim
RUN useradd -r -u 10001 queue && mkdir /data && chown queue /data
COPY --from=build /src/build/queue_server /usr/local/bin/queue_server
USER queue
ENV HOST=0.0.0.0 PORT=8081 JOURNAL_PATH=/data/queue.wal
EXPOSE 8081
ENTRYPOINT ["queue_server"]
