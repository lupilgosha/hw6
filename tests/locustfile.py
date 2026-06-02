"""
Load test for warehouse consumer HTTP API.
Run: locust -f tests/locustfile.py --headless -u 10 -r 2 -t 30s --host http://localhost:8000
"""
import uuid
from locust import HttpUser, task, between


class WarehouseUser(HttpUser):
    wait_time = between(0.5, 1.5)

    def on_start(self):
        self.product_id = str(uuid.uuid4())
        self.zone_id = str(uuid.uuid4())

    @task
    def check_inventory(self):
        with self.client.get(
            "/inventory",
            params={"product_id": self.product_id, "zone_id": self.zone_id},
            catch_response=True,
        ) as response:
            if response.status_code in (200, 404):
                response.success()
            else:
                response.failure(f"unexpected status {response.status_code}")

    @task
    def health_check(self):
        with self.client.get("/health", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"health check failed: {response.status_code}")
