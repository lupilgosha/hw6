# Текст защиты hw7

---

## 1. Вступление

«Я реализовал hw7 на базе hw6 — event-driven системы управления складом.
Система состоит из двух сервисов: WMS Producer и Consumer, брокера Kafka со Schema Registry,
базы данных Cassandra, а также мониторинга на Prometheus и Grafana.
Сейчас я покажу E2E демонстрацию: подниму систему, запущу CI, покажу тесты и метрики.»

---

## 2. Поднять систему

«Начнём. Поднимаю всю инфраструктуру одной командой:»

```bash
cd ~/hw7
docker compose up -d
```

«Пока поднимается — расскажу архитектуру.
У нас два сервиса: wms-producer генерирует складские события и публикует их в Kafka topic warehouse-events.
Consumer читает события, обрабатывает их и сохраняет состояние в Cassandra.
Оба сервиса экспортируют метрики в Prometheus-совместимом формате на эндпоинте /metrics.
Также у consumer есть HTTP API — /inventory для получения остатков товара.»

«Дождёмся готовности consumer:»

```bash
for i in $(seq 1 40); do curl -sf http://localhost:8000/health && echo " ready" && break || echo "attempt $i..."; sleep 5; done
```

---

## 3. Показать метрики сервисов

«Consumer и producer уже отдают метрики. Проверим:»

```bash
curl -s http://localhost:8000/metrics | grep kafka_requests_total
```

«Видим три метрики: kafka_requests_total, kafka_request_errors_total, kafka_request_duration_seconds —
с labels method, endpoint, status. Это ровно то, что требует задание.»

```bash
curl -s http://localhost:8001/metrics | grep kafka_requests_total
```

---

## 4. Prometheus

«Открываем Prometheus: http://localhost:9090»

«Проверим, что все таргеты UP:»

```bash
curl -s "http://localhost:9090/api/v1/targets" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(t['labels']['job'], t['health']) for t in d['data']['activeTargets']]"
```

«Видим три таргета: warehouse-consumer, warehouse-producer, kafka-exporter — все up.»

«Запросим метрику в Prometheus:»

```bash
curl -s "http://localhost:9090/api/v1/query?query=kafka_requests_total" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(r['metric']['job'], r['metric']['status'], r['value'][1]) for r in d['data']['result']]"
```

---

## 5. Grafana — дашборды

«Открываем Grafana: http://localhost:3000 (admin/admin)»

«Дашборды загружаются автоматически через provisioning — никаких ручных действий не нужно.»

```bash
curl -s "http://localhost:3000/api/search" -u admin:admin | python3 -c "import sys,json; [print(d['title']) for d in json.load(sys.stdin)]"
```

«Два дашборда:
- Warehouse Services — throughput, error rate, latency p50/p95/p99, requests by status
- Warehouse Infrastructure — Kafka consumer lag, messages/s, broker status, lag by partition»

«Открываем Warehouse Services в браузере. Видим 4 панели с реальными данными.»

---

## 6. Unit-тесты

«Запускаю unit-тесты. Они не требуют docker — тестируют бизнес-логику consumer в изоляции через моки:»

```bash
cd ~/hw7
python3 -m pytest tests/test_consumer_unit.py -v
```

«6 тестов: валидация событий, обработка невалидного количества, неизвестный тип события,
игнорирование устаревших событий, идемпотентность — дубликаты не обрабатываются повторно.»

---

## 7. Интеграционные тесты

«Интеграционные тесты проверяют взаимодействие между сервисами — не один сервис в изоляции:»

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
SCHEMA_REGISTRY_URL=http://localhost:8081 \
CASSANDRA_CONTACT_POINTS=localhost \
WAREHOUSE_TOPIC=warehouse-events \
python3 -m pytest tests/test_integration.py -v
```

«Три теста:
1. Producer → Schema Registry → Kafka: событие сериализуется через Avro и доставляется
2. Kafka → Consumer → Cassandra: событие обрабатывается и запись появляется в БД
3. Невалидное событие → DLQ: consumer не падает, событие уходит в warehouse-events-dlq»

---

## 8. E2E тест

«E2E тест проверяет полный пользовательский сценарий сквозно:»

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
SCHEMA_REGISTRY_URL=http://localhost:8081 \
CASSANDRA_CONTACT_POINTS=localhost \
CONSUMER_API_URL=http://localhost:8000 \
WAREHOUSE_TOPIC=warehouse-events \
python3 -m pytest tests/test_e2e.py -v
```

«Сценарий: отправляем PRODUCT_RECEIVED с quantity=100,
ждём пока consumer обработает,
делаем GET /inventory?product_id=...&zone_id=... — проверяем HTTP статус 200,
проверяем поля и типы в теле ответа,
затем идём напрямую в Cassandra и проверяем что available_quantity=100.»

---

## 9. Нагрузочный тест

«Нагрузочный тест интегрирован в CI. Запущу вручную:»

```bash
python3 -m locust -f tests/locustfile.py \
  --headless --users 10 --spawn-rate 2 --run-time 30s \
  --host http://localhost:8000 --exit-code-on-error 1
```

«10 виртуальных пользователей, 30 секунд. Тест проверяет /inventory и /health.
При превышении порогов ошибок — exit code 1 и CI падает.
После теста проверяем что сервис жив:»

```bash
curl -s http://localhost:8000/health
```

---

## 10. CI pipeline

«CI pipeline в .github/workflows/ci.yml запускается автоматически при push или PR.»

«Структура пайплайна:»

```
build → unit-tests → integration-tests → e2e-tests → load-tests
```

«Каждый шаг последовательный — следующий запускается только при успехе предыдущего.
При любой ошибке пайплайн падает.
Результаты нагрузочного теста сохраняются как артефакт (HTML-отчёт).»

«Покажу конфиг:»

```bash
cat ~/hw7/.github/workflows/ci.yml
```

---

## 11. Возможные вопросы и ответы

**Q: Почему at-least-once семантика, а не exactly-once?**
A: Exactly-once в Kafka требует транзакций на стороне producer и idempotent consumer.
Мы реализовали идемпотентность через таблицу processed_events в Cassandra —
перед обработкой проверяем event_id, дубликаты пропускаем.
Это даёт нам семантику exactly-once на уровне бизнес-логики при at-least-once доставке.

**Q: Почему offset коммитится после записи в Cassandra, а не до?**
A: Если закоммитить offset до записи и сервис упадёт — событие потеряется навсегда.
Если закоммитить после — при рестарте событие придёт повторно, но идемпотентность его отфильтрует.

**Q: Что такое DLQ и зачем он нужен?**
A: Dead Letter Queue — отдельный Kafka topic warehouse-events-dlq.
Туда уходят события, которые не удалось обработать: невалидные данные, неизвестный тип события.
Это позволяет не блокировать обработку остальных событий и разобраться с проблемными позже.

**Q: Как работает Grafana provisioning?**
A: При старте Grafana читает YAML-файлы из /etc/grafana/provisioning/.
Datasource (Prometheus) и дашборды (JSON-файлы) загружаются автоматически.
Никаких ручных действий через UI не нужно — всё воспроизводимо из кода.

**Q: Что такое Avro и зачем Schema Registry?**
A: Avro — бинарный формат сериализации с явной схемой.
Schema Registry хранит версии схем и позволяет эволюционировать их без поломки совместимости.
Producer регистрирует схему, consumer получает её по ID из заголовка сообщения.

**Q: Чем интеграционный тест отличается от E2E?**
A: Интеграционный тест проверяет взаимодействие между конкретными компонентами:
producer→Kafka, Kafka→Cassandra, consumer→DLQ.
E2E тест проверяет полный пользовательский сценарий сквозно через внешний API —
как это делал бы реальный клиент системы.

**Q: Почему locust, а не k6?**
A: Locust написан на Python, что удобно в нашем Python-стеке.
Сценарии пишутся как обычный Python-код, легко интегрируется в CI через exit code.
k6 тоже хороший выбор, но требует отдельного бинаря.
