export PROJECT_DIR := $(CURDIR)

AIRFLOW := docker compose -f airflow/docker-compose.yaml
INFRA := docker compose -f extract_load/docker-compose.yml

.PHONY: airflow-up airflow-down airflow-ps airflow-logs infra-up infra-down test dq

airflow-up:
	$(AIRFLOW) up -d

airflow-down:
	$(AIRFLOW) down

airflow-ps:
	$(AIRFLOW) ps

airflow-logs:
	$(AIRFLOW) logs -f --tail 100

infra-up:
	$(INFRA) up -d

infra-down:
	$(INFRA) down

test:
	pytest -q

dq:
	python -m dataplatform dq
