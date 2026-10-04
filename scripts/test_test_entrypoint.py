"""Контракт собранного test-образа: защита предшествует выбранной команде."""

import subprocess

import pytest


@pytest.mark.parametrize(
    "override",
    [
        "TESTING=false",
        "ORDERMESH_ORDERS_DATABASE_URL=postgresql+psycopg://ordermesh:ordermesh@orders-db-test:5432/orders_test?host=database",
    ],
)
def test_test_image_refuses_unsafe_command(override):
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "test",
            "run",
            "--rm",
            "--no-deps",
            "-e",
            override,
            "test",
            "python",
            "-c",
            'print("UNSAFE_COMMAND_STARTED")',
        ],
        capture_output=True,
        text=True,
        timeout=90,
    )
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "Небезопасное тестовое окружение" in output
    assert "UNSAFE_COMMAND_STARTED" not in output
