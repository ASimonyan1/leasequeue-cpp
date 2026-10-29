# HTTP-сервер LeaseQueue

`queue_server` — отдельный процесс с HTTP-адаптером поверх прежнего ядра. Клиенты не задают текущее время: сервер берёт Unix milliseconds. Клиент получает ID и token аренды, передаёт token при ACK/NACK. Один файл принадлежит одному процессу; несколько обработчиков работают через HTTP.

## Запуск

```sh
cp .env.example .env
docker compose up --build -d
curl --fail --retry 40 --retry-all-errors --retry-delay 2 http://localhost:8081/health
```

PowerShell: `Copy-Item .env.example .env`, затем `curl.exe`. `.env.example` содержит только локальный демонстрационный токен. Compose публикует порт на localhost и сохраняет журнал в томе. Остановка `docker compose down` посылает SIGTERM; сервер прекращает приём соединений, дожидается обработчиков и освобождает файловую блокировку.

Без Docker: обычная CMake-сборка создаёт `queue_server` (`build/Debug/queue_server.exe` на Windows). Переменные: `API_TOKEN` (минимум 16 символов), `JOURNAL_PATH` (по умолчанию `queue.wal`), `HOST` (127.0.0.1), `PORT` (8081). Для библиотеки без сетевых зависимостей задайте `-DBUILD_HTTP_SERVER=OFF`.

## Пример

```sh
curl -H 'Authorization: Bearer local-queue-token-change-me' -H 'Content-Type: application/json' \
  -d '{"key":"mail-42","payload":"send reminder","max_attempts":3}' http://localhost:8081/v1/jobs
curl -H 'Authorization: Bearer local-queue-token-change-me' -H 'Content-Type: application/json' \
  -d '{"lease_ms":30000}' http://localhost:8081/v1/claims
```

Первый запрос возвращает `{"id":1}`. Ответ claim содержит `id`, `token`, payload и `lease_until`. Передайте **полученные** id и token, например:

```sh
curl -H 'Authorization: Bearer local-queue-token-change-me' -H 'Content-Type: application/json' \
  -d '{"token":2}' http://localhost:8081/v1/jobs/1/ack
```

## Контракт

Все `/v1/*` требуют Bearer token. JSON содержит ровно указанные поля.

| Запрос | Тело | Ответ |
|---|---|---|
| POST `/v1/jobs` | key, payload, max_attempts | 200 и стабильный ID, в том числе при повторе |
| GET `/v1/jobs/{id}` | — | Состояние задания; 404 при отсутствии |
| POST `/v1/claims` | lease_ms: 1–60000 | 201 и аренда; 204, если работы нет |
| POST `/v1/jobs/{id}/ack` | token | 200 при действующей аренде; иначе 409 |
| POST `/v1/jobs/{id}/nack` | token, backoff_ms: 0–60000 | 200 или 409 |
| GET `/v1/stats` | — | Число ready / leased / succeeded / dead |
| GET `/health` | — | Проверка доступности ядра без авторизации |

key: 1–128 байт; payload: до 4096 байт; max_attempts: 1–100. Числа только целые, bool не принимается. Новая отправка и идемпотентный повтор имеют общий статус 200; другой payload/бюджет под тем же key — 409. Неправильный запрос — 400; тело более 8 KiB — 413; отказ ядра — 503.

## Ограничения и проверки

Шесть HTTP-потоков, очередь до 32 ожидающих соединений, read/write timeout по 2 секунды. Блокирующий HTTP через cpp-httplib, не собственный epoll-сервер. При переполнении соединение может быть закрыто. Токен один на все операции: разграничения работников и пользователей нет. TLS и публичное развёртывание не входят в пример.

`tests/http_tests.py` поднимает отдельный процесс/журнал для каждого теста: авторизация и пределы, идемпотентность, конкурентный claim, истёкший token, бюджет повторов, восстановление после принудительной остановки. HTTP-тест срока использует ожидание; точные границы времени без sleep проверяются прежними тестами ядра. После аварии lock снимается тестом только в своём временном каталоге и после подтверждённого выхода процесса.

Журнал и его прежние гарантии не изменены: полный снимок на изменение, без компактации и fsync. Восстановление проверяется после остановки процесса, сохранность при потере питания не обещается. Истёкшие lease преобразуются при claim, поэтому GET/stats до claim могут показывать leased для уже просроченного задания.
