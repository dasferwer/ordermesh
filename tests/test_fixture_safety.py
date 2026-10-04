import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SAFE_ENV = {
    "ORDERMESH_API_KEYS": '{"test":"test-client-key-long","other":"other-client-key-long"}',
    "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@inventory-db-test:5432/inventory_test",
    "ORDERMESH_MAX_RETRIES": "3",
    "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@orders-db-test:5432/orders_test",
    "ORDERMESH_RABBITMQ_URL": "amqp://ordermesh:ordermesh@rabbitmq-test:5672/%2F",
    "ORDERMESH_RETRY_DELAY_MS": "100",
    "TESTING": "true",
}
INVALID_ENVIRONMENTS = [
    {"TESTING": None},
    {"TESTING": "false"},
    {"TESTING": "1"},
    {"TESTING": "TRUE"},
    {"PGHOSTADDR": "127.0.0.1"},
    {"PGSERVICE": "ordinary"},
    {"PGOPTIONS": "-c search_path=ordinary"},
    {"MIGRATION_DATABASE_URL": "postgresql+asyncpg://owner:invented@database:5432/shared"},
    {"ORDERMESH_ORDERS_DATABASE_URL": None},
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@database:5432/orders_test"
    },
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@orders-db-test.invalid:5432/orders_test"
    },
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@orders-db-test:5433/orders_test"
    },
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@orders-db-test:5432/ordinary"
    },
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql://ordermesh:ordermesh@orders-db-test:5432/orders_test"
    },
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@orders-db-test:5432/orders_test?host=database"
    },
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@orders-db-test:5432/orders_test?hostaddr=127.0.0.1"
    },
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@orders-db-test:5432/orders_test?dbname=ordinary"
    },
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@orders-db-test:5432/orders_test?database=ordinary"
    },
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@orders-db-test:5432/orders_test#ordinary"
    },
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@orders-db-test:5432/orders_test\n"
    },
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://postgres:ordermesh@orders-db-test:5432/orders_test"
    },
    {
        "ordermesh_orders_database_url": "postgresql+psycopg://ordermesh:ordermesh@orders-db-test:5432/ordinary"
    },
    {
        "ORDERMESH_ORDERS_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@orders-db-test:5432/orders%5Ftest"
    },
    {"ORDERMESH_INVENTORY_DATABASE_URL": None},
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@database:5432/inventory_test"
    },
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@inventory-db-test.invalid:5432/inventory_test"
    },
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@inventory-db-test:5433/inventory_test"
    },
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@inventory-db-test:5432/ordinary"
    },
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql://ordermesh:ordermesh@inventory-db-test:5432/inventory_test"
    },
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@inventory-db-test:5432/inventory_test?host=database"
    },
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@inventory-db-test:5432/inventory_test?hostaddr=127.0.0.1"
    },
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@inventory-db-test:5432/inventory_test?dbname=ordinary"
    },
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@inventory-db-test:5432/inventory_test?database=ordinary"
    },
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@inventory-db-test:5432/inventory_test#ordinary"
    },
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@inventory-db-test:5432/inventory_test\n"
    },
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://postgres:ordermesh@inventory-db-test:5432/inventory_test"
    },
    {
        "ordermesh_inventory_database_url": "postgresql+psycopg://ordermesh:ordermesh@inventory-db-test:5432/ordinary"
    },
    {
        "ORDERMESH_INVENTORY_DATABASE_URL": "postgresql+psycopg://ordermesh:ordermesh@inventory-db-test:5432/inventory%5Ftest"
    },
    {"ORDERMESH_RABBITMQ_URL": None},
    {"ORDERMESH_RABBITMQ_URL": "amqp://ordermesh:ordermesh@ordinary:5672/%2F"},
    {"ORDERMESH_RABBITMQ_URL": "amqp://ordermesh:ordermesh@rabbitmq-test:5672/%2F?host=ordinary"},
    {"ORDERMESH_RABBITMQ_URL": "amqp://ordermesh:ordermesh@rabbitmq-test:5672/%2F#ordinary"},
    {"ORDERMESH_RABBITMQ_URL": "amqp://ordermesh:ordermesh@rabbitmq-test:5673/%2F"},
]
PACKAGE = "ordermesh"
COLLECTION_TARGET = "tests/test_fixture_safety.py"
MIGRATION_SCRIPT = "scripts/check_migration.py"

# Если защита отсутствует, стоит поздно или проверяет текст URL вместо ресурса,
# настоящий сборщик pytest/миграционный скрипт нарушит границу до побочных действий.
PROBE = r"""
import importlib.abc, json, runpy, subprocess, sys
from pathlib import Path

marker, package, mode, target, protect_imports = sys.argv[1:]
actions = []
class Boundary(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if protect_imports == 'yes' and fullname in {
            package + '.db', package + '.config', package + '.main', package + '.seed'
        }:
            actions.append('application_import:' + fullname)
            raise RuntimeError('Приложение импортировано до проверки окружения')
sys.meta_path.insert(0, Boundary())
def audit(event, args):
    if event in {'socket.connect', 'subprocess.Popen', 'os.system'}:
        actions.append(event)
        raise RuntimeError('Побочное действие запрещено в проверке защиты')
sys.addaudithook(audit)
try:
    if mode == 'collect':
        import pytest
        code = pytest.main(['--collect-only', '-q', '-p', 'no:cacheprovider', target])
        raise SystemExit(code)
    if mode == 'migration':
        runpy.run_path(target, run_name='__main__')
    elif mode == 'guard':
        runpy.run_module(package + '.test_safety', run_name='__main__')
finally:
    Path(marker).write_text(json.dumps(actions))
"""


def run_probe(tmp_path, overrides, mode="collect", protect_imports=True):
    # Не переносим секреты и параметры маршрутизации настоящего окружения в child.
    environment = {
        k: v for k, v in os.environ.items() if k in {"PATH", "HOME", "TMPDIR", "LANG", "SYSTEMROOT"}
    }
    environment.update(SAFE_ENV)
    root = Path(__file__).resolve().parents[1]
    environment["PYTHONPATH"] = str(root / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    for key, value in overrides.items():
        if value is None:
            environment.pop(key, None)
        else:
            environment[key] = value
    marker = tmp_path / "actions.json"
    target = MIGRATION_SCRIPT if mode == "migration" else COLLECTION_TARGET
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            PROBE,
            str(marker),
            PACKAGE,
            mode,
            target,
            "yes" if protect_imports else "no",
        ],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result, json.loads(marker.read_text())


@pytest.mark.parametrize("overrides", INVALID_ENVIRONMENTS)
def test_unsafe_environment_is_rejected_before_imports_and_actions(tmp_path, overrides):
    result, actions = run_probe(tmp_path, overrides)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "Небезопасное тестовое окружение" in result.stdout + result.stderr
    assert actions == []


def test_isolated_environment_can_be_collected_without_network(tmp_path):
    result, actions = run_probe(tmp_path, {}, protect_imports=False)
    assert result.returncode == 0, result.stdout + result.stderr
    assert actions == []


@pytest.mark.parametrize("query", ["?host=database", "?database=ordinary", "?dbname=ordinary"])
def test_guard_command_rejects_query_before_any_action(tmp_path, query):
    key = next(
        key for key in SAFE_ENV if key.endswith("DATABASE_URL") and key != "MIGRATION_DATABASE_URL"
    )
    result, actions = run_probe(tmp_path, {key: SAFE_ENV[key] + query}, mode="guard")
    assert result.returncode != 0
    assert "Небезопасное тестовое окружение" in result.stdout + result.stderr
    assert actions == []


def test_guard_command_accepts_isolated_environment(tmp_path):
    result, actions = run_probe(tmp_path, {}, mode="guard", protect_imports=False)
    assert result.returncode == 0, result.stdout + result.stderr
    assert actions == []


@pytest.mark.skipif(MIGRATION_SCRIPT == "", reason="Отдельного тестового мигратора нет")
@pytest.mark.parametrize("kind", ["query", "flag", "owner"])
def test_migration_script_refuses_before_import_or_process(tmp_path, kind):
    key = next(
        key for key in SAFE_ENV if key.endswith("DATABASE_URL") and key != "MIGRATION_DATABASE_URL"
    )
    overrides = (
        {key: SAFE_ENV[key] + "?host=database"}
        if kind == "query"
        else (
            {"TESTING": None}
            if kind == "flag"
            else {
                "MIGRATION_DATABASE_URL": "postgresql+asyncpg://owner:invented@database:5432/shared"
            }
        )
    )
    result, actions = run_probe(tmp_path, overrides, mode="migration")
    assert result.returncode != 0
    assert "Небезопасное тестовое окружение" in result.stdout + result.stderr
    assert actions == []


@pytest.mark.parametrize(
    "variable, delimiter",
    [
        ("ORDERMESH_ORDERS_DATABASE_URL", "?"),
        ("ORDERMESH_ORDERS_DATABASE_URL", "#"),
        ("ORDERMESH_INVENTORY_DATABASE_URL", "?"),
        ("ORDERMESH_INVENTORY_DATABASE_URL", "#"),
    ],
)
def test_empty_url_delimiters_are_rejected_before_import(tmp_path, variable, delimiter):
    base_url = SAFE_ENV.get(variable, SAFE_ENV["ORDERMESH_ORDERS_DATABASE_URL"])
    result, actions = run_probe(tmp_path, {variable: base_url + delimiter})
    assert result.returncode != 0
    assert "Небезопасное тестовое окружение" in result.stdout + result.stderr
    assert actions == []
