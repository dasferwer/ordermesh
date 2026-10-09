"""Proof отвергает обычный стенд до запуска Docker и записи данных."""

import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "broker_outage", Path(__file__).with_name("broker_outage.py")
)
assert spec and spec.loader
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)


@pytest.mark.parametrize(
    "project", ["ordermesh", "", "proof-ordermesh-", "proof-ordermesh-X", "proof-ordermesh-a/b"]
)
def test_ordinary_project_refused(project):
    with pytest.raises(ValueError):
        proof.validate_options(project, "10.242.4.0/24", 30, 10)


@pytest.mark.parametrize(
    "subnet,seconds,count",
    [
        ("8.8.8.0/24", 30, 10),
        ("10.242.0.0/16", 30, 10),
        ("::1/128", 30, 10),
        ("127.0.0.0/24", 30, 10),
        ("10.242.4.0/24", 0, 10),
        ("10.242.4.0/24", 301, 10),
        ("10.242.4.0/24", 30, 21),
    ],
)
def test_unbounded_options_refused(subnet, seconds, count):
    with pytest.raises(ValueError):
        proof.validate_options("proof-ordermesh-ci", subnet, seconds, count)


def test_existing_project_is_not_mutated(monkeypatch):
    commands = []

    def docker(command, **kwargs):
        commands.append(command)
        return type("Result", (), {"stdout": "existing-container\n"})()

    monkeypatch.setattr(proof, "run", docker)
    network = proof.validate_options("proof-ordermesh-ci", "10.242.4.0/24", 30, 10)
    with pytest.raises(ValueError, match="занято"):
        proof.preflight("proof-ordermesh-ci", network)
    assert len(commands) == 1
    assert commands[0][1:3] == ["container", "ls"]


def test_cleanup_continues_after_logs_failure(monkeypatch):
    commands = []

    def compose(*args, **kwargs):
        commands.append(args)
        if args[0] == "logs":
            raise RuntimeError("logs недоступны")

    monkeypatch.setattr(proof, "run", lambda *args, **kwargs: None)
    errors = proof.cleanup(compose, "proof-ordermesh-ci:runtime")
    assert commands[-1] == ("down", "-v", "--remove-orphans")
    assert len(errors) == 1
