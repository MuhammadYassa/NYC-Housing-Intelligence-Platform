"""Separate invalid source contents from retryable execution failures."""


class InvalidArtifact(ValueError):
    """The acquired bytes violate an independent source requirement."""


class OperationalError(RuntimeError):
    """The environment could not execute an otherwise valid candidate."""


class MissingGeodatabase(FileNotFoundError):
    """The archive is readable but contains no FileGDB."""
