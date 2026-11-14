import csv
import gzip
import json
import os
import sys
import time
from collections import Counter
from itertools import combinations, islice
from pathlib import Path

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError, RolledBackError

HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
CHUNK_SIZE = 1000
PAUSE_AFTER = 10_000


def commit(transaction):
    for value in transaction.commit():
        if isinstance(value, Exception) and not isinstance(value, RolledBackError):
            raise value


def claim_leadership(zk, root, name):
    while True:
        _, stat = zk.get(f"{root}/epoch")
        epoch = stat.version + 1

        transaction = zk.transaction()
        transaction.create(f"{root}/leader", name.encode(), ephemeral=True)
        transaction.set_data(
            f"{root}/epoch", str(epoch).encode(), version=stat.version
        )

        try:
            commit(transaction)
        except (NodeExistsError, BadVersionError):
            time.sleep(0.2)
        else:
            return epoch


def count_pairs(name, folder):
    checkpoint = folder / "checkpoint.json.gz"
    counts = Counter()
    processed = selected = with_skills = 0

    if name == "B":
        with gzip.open(checkpoint, "rt", encoding="utf-8") as file:
            processed, selected, with_skills, saved = json.load(file)
        counts.update({(a, b): count for a, b, count in saved})
        print(f"Восстановил чекпоинт, обработано {processed} строк", flush=True)

    source = Path("data/it_vacancies_2024.csv")
    with source.open(encoding="utf-8-sig", newline="") as file:
        rows = csv.DictReader(file)
        for _ in range(processed):
            next(rows)

        while chunk := list(islice(rows, CHUNK_SIZE)):
            partial = Counter()
            for row in chunk:
                if row["role"] != "Программист, разработчик":
                    continue
                if "python" not in (row["name"] or "").lower():
                    continue

                selected += 1
                skills = sorted({
                    skill.strip()
                    for skill in (row["key_skills"] or "").split(";")
                    if skill.strip()
                })
                with_skills += bool(skills)
                partial.update(combinations(skills, 2))

            counts.update(partial)
            processed += len(chunk)

            if name == "A" and processed == PAUSE_AFTER:
                temporary = checkpoint.with_suffix(".tmp")
                saved = [[a, b, count] for (a, b), count in counts.items()]

                with gzip.open(temporary, "wt", encoding="utf-8") as file:
                    json.dump([processed, selected, with_skills, saved], file)

                temporary.replace(checkpoint)
                print(
                    f"Сохранил чекпоинт после {processed} строк. ГОТОВ К SIGSTOP",
                    flush=True,
                )

                while not (folder / "resume").exists():
                    time.sleep(0.1)

                print("Продолжил расчёт после SIGCONT", flush=True)

    print(
        f"Всего={processed}, по фильтру={selected}, с навыками={with_skills}, "
        f"уникальных пар={len(counts)}, "
        f"PostgreSQL + Python={counts['PostgreSQL', 'Python']}",
        flush=True,
    )
    return counts


def publish(root, name, folder, epoch, fencing, counts):
    result = folder / f"skills_2-{name}.csv"
    with result.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["skills", "count"])
        for pair, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
            writer.writerow(["; ".join(pair), count])

    payload = json.dumps({
        "coordinator": name,
        "epoch": epoch,
        "file": str(result),
    }).encode()

    receiver = KazooClient(hosts=HOSTS, timeout=10)
    receiver.start(timeout=15)
    try:
        if fencing:
            transaction = receiver.transaction()
            transaction.check(f"{root}/epoch", epoch)
            transaction.set_data(f"{root}/result", payload)
            commit(transaction)
        else:
            receiver.set(f"{root}/result", payload)

        _, stat = receiver.get(f"{root}/result")
        print(
            f"{name} записал результат, epoch={epoch}, dataVersion={stat.version}",
            flush=True,
        )
    except BadVersionError:
        result.unlink()
        print(f"{name} BadVersionError: запись отклонена, epoch={epoch}", flush=True)
    finally:
        receiver.stop()
        receiver.close()


def main():
    name, run = sys.argv[1:]
    fencing = os.environ.get("FENCING") == "1"
    root = f"/lab4/{run}"
    folder = Path("runs") / run

    zk = KazooClient(hosts=HOSTS, timeout=10)
    zk.start(timeout=15)
    try:
        if name == "A":
            folder.mkdir(parents=True)
            zk.create(root, makepath=True)
            zk.create(f"{root}/epoch", b"0")
            zk.create(f"{root}/result", b"null")
            (folder / "A.pid").write_text(str(os.getpid()))

        print(
            f"{name} ожидает лидерства, FENCING={int(fencing)}, тайм-аут сессии 10 с",
            flush=True,
        )
        epoch = claim_leadership(zk, root, name)
        leader_ns = time.monotonic_ns()
        print(f"{name} стал лидером, epoch={epoch}", flush=True)

        if name == "B":
            stop_ns = int((folder / "stop-ns").read_text())
            print(f"Смена лидера: {(leader_ns - stop_ns) / 1e9:.3f} с", flush=True)

        counts = count_pairs(name, folder)
        publish(root, name, folder, epoch, fencing, counts)
    finally:
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()