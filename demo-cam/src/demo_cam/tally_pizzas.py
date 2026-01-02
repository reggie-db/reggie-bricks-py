#!/usr/bin/env python3
import json
import math
import sys

import numpy as np

TOTAL_SLICES = 8
SLICE_ARC = 2 * math.pi / TOTAL_SLICES
SLICE_ARC_FACTOR = 0.9
SLICE_ARC_WIDTH = SLICE_ARC * SLICE_ARC_FACTOR


def load_predictions(json_str: str) -> list[dict]:
    try:
        if predictions := json.loads(json_str).get("predictions", []):
            return predictions
    except json.JSONDecodeError:
        pass
    print("invalid json")
    return []


def cluster_pizzas(preds: list[dict]) -> list[list[dict]]:
    if not preds:
        return []

    areas = np.array([p["width"] * p["height"] for p in preds])
    max_area = areas.max()

    # Step 1: find pizza anchors (large boxes)
    ANCHOR_RATIO = 0.35
    anchors = [p for p, a in zip(preds, areas) if a >= ANCHOR_RATIO * max_area]

    # Fallback: if no anchors, use largest boxes
    if not anchors:
        idx = np.argsort(-areas)[:3]
        anchors = [preds[i] for i in idx]

    clusters = {id(a): [a] for a in anchors}

    # Step 2: assign remaining boxes to nearest anchor
    for p in preds:
        if p in anchors:
            continue

        px, py = p["x"], p["y"]
        nearest = min(anchors, key=lambda a: (a["x"] - px) ** 2 + (a["y"] - py) ** 2)
        clusters[id(nearest)].append(p)

    return list(clusters.values())


def estimate_center(cluster: list[dict]) -> tuple[float, float]:
    xs = [p["x"] for p in cluster]
    ys = [p["y"] for p in cluster]
    return float(np.median(xs)), float(np.median(ys))


def slice_boxes(cluster: list[dict]) -> list[dict]:
    areas = [p["width"] * p["height"] for p in cluster]
    max_area = max(areas)
    return [p for p, a in zip(cluster, areas) if a < 0.25 * max_area]


def angle_intervals(
    cluster: list[dict], cx: float, cy: float
) -> list[tuple[float, float]]:
    intervals = []
    for p in cluster:
        angle = math.atan2(p["y"] - cy, p["x"] - cx)
        a = (angle + 2 * math.pi) % (2 * math.pi)
        start = a - SLICE_ARC_WIDTH / 2
        end = a + SLICE_ARC_WIDTH / 2
        intervals.append((start, end))
    return intervals


def merge_intervals(intervals: list[tuple[float, float]]) -> float:
    if not intervals:
        return 0.0

    expanded = []
    for s, e in intervals:
        if s < 0:
            expanded.append((s + 2 * math.pi, 2 * math.pi))
            expanded.append((0, e))
        elif e > 2 * math.pi:
            expanded.append((s, 2 * math.pi))
            expanded.append((0, e - 2 * math.pi))
        else:
            expanded.append((s, e))

    expanded.sort()
    covered = 0.0
    cur_start, cur_end = expanded[0]

    for s, e in expanded[1:]:
        if s <= cur_end:
            cur_end = max(cur_end, e)
        else:
            covered += cur_end - cur_start
            cur_start, cur_end = s, e

    covered += cur_end - cur_start
    return covered


def estimate_slices_for_cluster(cluster: list[dict]) -> int:
    cx, cy = estimate_center(cluster)
    slices = slice_boxes(cluster)

    if not slices:
        return TOTAL_SLICES

    intervals = angle_intervals(slices, cx, cy)
    covered = merge_intervals(intervals)

    missing = int(round((2 * math.pi - covered) / SLICE_ARC))
    remaining = TOTAL_SLICES - missing
    return max(0, min(TOTAL_SLICES, remaining))


def process_predictions(preds: list[dict]):
    clusters = cluster_pizzas(preds)

    results = []
    for cluster in sorted(clusters, key=lambda c: np.median([p["x"] for p in c])):
        results.append(estimate_slices_for_cluster(cluster))

    print(json.dumps({"pizza_count": len(results), "slices_per_pizza": results}))


def main():
    while True:
        lines = []
        for line in sys.stdin:
            if line := line.strip():
                lines.append(line)
            else:
                break
        if not lines:
            continue
        preds = load_predictions("\n".join(lines))
        if not preds:
            continue
        process_predictions(preds)


if __name__ == "__main__":
    main()
