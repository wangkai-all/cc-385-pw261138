"""One module per process, so the three stages really run independently.

    python -m pipeline.ingester --data ./data --account alpha --amount 450
    python -m pipeline.worker_process --data ./data --interval 0.2
    python -m pipeline.publisher_process --data ./data --interval 0.2

They share only the on-disk layout (see ``store``): the input JSONL is the
contract between ingester and worker, the contributions JSONL between worker
and publisher.
"""
