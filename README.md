# Smart Warehouse — hw7

Event-driven система управления складом с CI/CD, мониторингом и нагрузочным тестированием.

## Архитектура

```
WMS Producer ──► Kafka ──► Consumer ──► Cassandra
     │              │           │
     │         Schema Registry  ├── /metrics (Prometheus)
     │                          ├── /health
     │                          └── /inventory
     │
Prometheus ◄── scrape ──────────┘
     │
Grafana ◄── dashboards
```

**Сервисы:**
- `wms-producer` — генерирует складские события, экспортирует метрики на `:8001/metrics`
- `consumer` — читает события из Kafka, обновляет Cassandra, экспортирует метрики на `:8000/metrics`, предоставляет HTTP API
- `kafka` + `schema-registry` — брокер сообщений с Avro-схемами
- `cassandra` — хранилище состояния склада
- `kafka-exporter` — экспортирует метрики Kafka для Prometheus
- `prometheus` — сбор метрик (`:9090`)
- `grafana` — дашборды (`:3000`, логин `admin/admin`)

## Запуск

```bash
docker compose up -d
```

## Метрики

Каждый сервис экспортирует три метрики:

| Метрика | Тип | Labels |
|---|---|---|
| `kafka_requests_total` | Counter | method, endpoint, status |
| `kafka_request_errors_total` | Counter | method, endpoint, error_type |
| `kafka_request_duration_seconds` | Histogram | method, endpoint |

## Тесты

```bash
pip install -r requirements.txt

pytest tests/test_consumer_unit.py -v

pytest tests/test_integration.py -v

pytest tests/test_e2e.py -v

locust -f tests/locustfile.py --headless -u 10 -r 2 -t 30s --host http://localhost:8000
```

## CI/CD

GitHub Actions pipeline (`.github/workflows/ci.yml`):

```
build → unit-tests → integration-tests → e2e-tests → load-tests
```

Пайплайн падает при любой ошибке. Результаты нагрузочного теста сохраняются как артефакт.

## Модель данных Cassandra

| Таблица | Partition Key | Clustering Key | Назначение |
|---|---|---|---|
| `inventory_by_product_zone` | `(product_id, zone_id)` | — | Остаток товара в конкретной зоне |
| `inventory_by_product` | `product_id` | `zone_id` | Все зоны для товара |
| `inventory_by_zone` | `zone_id` | `product_id` | Все товары в зоне |
| `processed_events` | `event_id` | — | Идемпотентность |
| `orders` | `order_id` | — | Статус заказов |
| `event_history` | `product_id` | `event_timestamp, event_id` | Аудит событий |

## HTTP API consumer

| Endpoint | Описание |
|---|---|
| `GET /health` | Проверка доступности |
| `GET /metrics` | Prometheus метрики |
| `GET /inventory?product_id=&zone_id=` | Остатки товара в зоне |
