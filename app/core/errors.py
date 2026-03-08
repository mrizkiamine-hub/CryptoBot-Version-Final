class DbUnavailableError(RuntimeError):
    """Raised when Postgres is not reachable / query fails."""
    pass
