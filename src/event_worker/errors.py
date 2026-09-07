class QueueError(Exception):
    """Base error for queue operations."""


class JobNotFoundError(QueueError):
    def __init__(self, job_id: str) -> None:
        super().__init__(f"job not found: {job_id}")
        self.job_id = job_id


class StaleLeaseError(QueueError):
    """Ack/fail rejected because this worker no longer owns the lease."""

    def __init__(self, job_id: str, reason: str = "stale lease") -> None:
        super().__init__(f"{reason}: {job_id}")
        self.job_id = job_id


class JobStateError(QueueError):
    def __init__(self, job_id: str, message: str) -> None:
        super().__init__(f"{message}: {job_id}")
        self.job_id = job_id
