import argparse
import json
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from uuid import uuid4


@dataclass(frozen=True)
class Result:
    status: int
    latency_ms: float


def send_order(base_url: str, sequence: int) -> Result:
    payload = json.dumps(
        {
            "customer_email": f"load-{sequence}@example.com",
            "items": [{"sku": "LICENSE-PRO", "quantity": 1}],
        }
    ).encode()
    request = urllib.request.Request(
        f"{base_url}/api/v1/orders",
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Idempotency-Key": f"load-{sequence}-{uuid4()}",
            "X-Correlation-ID": f"load-{sequence}",
        },
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            status = response.status
    except urllib.error.HTTPError as exc:
        status = exc.code
    return Result(status=status, latency_ms=(time.perf_counter() - started) * 1000)


def main() -> None:
    parser = argparse.ArgumentParser(description="OrderMesh fixed-rate smoke load")
    parser.add_argument("--url", default="http://localhost:8040")
    parser.add_argument("--rps", type=int, default=25)
    parser.add_argument("--seconds", type=int, default=10)
    args = parser.parse_args()
    total = args.rps * args.seconds
    started = time.perf_counter()
    futures = []
    with ThreadPoolExecutor(max_workers=max(args.rps * 2, 20)) as executor:
        for sequence in range(total):
            due = started + sequence / args.rps
            time.sleep(max(0, due - time.perf_counter()))
            futures.append(executor.submit(send_order, args.url, sequence))
        results = [future.result() for future in as_completed(futures)]
    elapsed = time.perf_counter() - started
    successful = sum(result.status in {200, 201} for result in results)
    latencies = sorted(result.latency_ms for result in results)
    p95 = latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))]
    print(
        f"requests={total} successful={successful} elapsed={elapsed:.2f}s "
        f"actual_rps={total / elapsed:.1f} p95_ms={p95:.1f}"
    )


if __name__ == "__main__":
    main()
