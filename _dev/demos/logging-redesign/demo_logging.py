"""Demo: Logging Redesign — show all new logging behaviors.

Demonstrates:
- Prefect env var suppression (PREFECT_LOGGING_LEVEL)
- Auto-configure via PipelineManager (no manual configure_logging needed)
- INFO vs DEBUG verbosity (demoted internal messages)
- File handler with DEBUG + logs_root
- artisan.tools logger (external tool output routed through logger)
- Idempotent calls (safe to call configure_logging multiple times)
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
from pathlib import Path


def _banner(title: str) -> None:
    width = 70
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print(f"{'=' * width}\n")


def _reset_logging() -> None:
    """Reset artisan logger state between demos."""
    artisan_logger = logging.getLogger("artisan")
    for handler in artisan_logger.handlers:
        handler.close()
    artisan_logger.handlers.clear()
    artisan_logger.setLevel(logging.WARNING)
    artisan_logger.propagate = True
    os.environ.pop("PREFECT_LOGGING_LEVEL", None)


def demo_prefect_env_var() -> None:
    """Show that suppress_noise sets PREFECT_LOGGING_LEVEL for child processes."""
    _reset_logging()
    _banner("Prefect env var suppression")

    from artisan.utils.logging import configure_logging

    print(f"Before: PREFECT_LOGGING_LEVEL = {os.environ.get('PREFECT_LOGGING_LEVEL', '(not set)')}")

    configure_logging(suppress_noise=True)
    print(f"After configure_logging(suppress_noise=True): PREFECT_LOGGING_LEVEL = {os.environ['PREFECT_LOGGING_LEVEL']}")

    # Show setdefault semantics — doesn't overwrite
    _reset_logging()
    os.environ["PREFECT_LOGGING_LEVEL"] = "WARNING"
    configure_logging(suppress_noise=True)
    print(f"With pre-existing value 'WARNING': PREFECT_LOGGING_LEVEL = {os.environ['PREFECT_LOGGING_LEVEL']}")
    print("  -> setdefault preserves user's choice")

    # Without suppress_noise
    _reset_logging()
    configure_logging(suppress_noise=False)
    print(f"After suppress_noise=False: PREFECT_LOGGING_LEVEL = {os.environ.get('PREFECT_LOGGING_LEVEL', '(not set)')}")

    print("\nChild processes (SLURM workers) inherit the env var, so Prefect")
    print("lifecycle logs are suppressed without needing per-process config.")


def demo_info_vs_debug() -> None:
    """Show that INFO is now clean — internal details are at DEBUG."""
    _reset_logging()
    _banner("INFO vs DEBUG verbosity")

    from artisan.utils.logging import configure_logging

    logger = logging.getLogger("artisan.orchestration.engine.step_executor")

    print("--- At INFO level (default) ---")
    configure_logging(level="INFO", suppress_noise=False)
    logger.info("Step 0 (DataGenerator): 10 artifacts generated")
    logger.debug("Step 0 (DataGenerator): resolved 10 input artifacts")
    logger.debug("Step 0 (DataGenerator): 10 artifacts -> 2 execution units")
    logger.debug("Step 0 (DataGenerator): 1 units cached, 1 to dispatch")
    print("  (debug messages are hidden at INFO level)\n")

    _reset_logging()
    print("--- At DEBUG level ---")
    configure_logging(level="DEBUG", suppress_noise=False)
    logger.info("Step 0 (DataGenerator): 10 artifacts generated")
    logger.debug("Step 0 (DataGenerator): resolved 10 input artifacts")
    logger.debug("Step 0 (DataGenerator): 10 artifacts -> 2 execution units")
    logger.debug("Step 0 (DataGenerator): 1 units cached, 1 to dispatch")
    print("  (all messages visible at DEBUG level)")


def demo_file_handler() -> None:
    """Show file handler creation with DEBUG + logs_root."""
    _reset_logging()
    _banner("File handler (DEBUG + logs_root)")

    from artisan.utils.logging import configure_logging

    with tempfile.TemporaryDirectory() as tmpdir:
        logs_root = Path(tmpdir) / "logs"

        print(f"logs_root: {logs_root}")
        print(f"Exists before: {logs_root.exists()}")

        configure_logging(level="DEBUG", suppress_noise=False, logs_root=logs_root)

        print(f"Exists after: {logs_root.exists()}")
        print(f"pipeline.log exists: {(logs_root / 'pipeline.log').exists()}")

        # Log some messages
        logger = logging.getLogger("artisan.demo")
        logger.debug("This DEBUG message goes to both console and file")
        logger.info("This INFO message goes to both console and file")

        # Show file contents
        log_content = (logs_root / "pipeline.log").read_text()
        print(f"\n--- File contents ({logs_root / 'pipeline.log'}) ---")
        print(log_content.rstrip())
        print("--- end ---")

        # Show that INFO + logs_root does NOT create a file handler
        _reset_logging()
        logs_root_2 = Path(tmpdir) / "logs2"
        configure_logging(level="INFO", suppress_noise=False, logs_root=logs_root_2)
        print(f"\nWith level='INFO': logs2/ created? {logs_root_2.exists()}")
        print("  (file handler only activates at DEBUG level)")

    # Show handler list
    artisan_logger = logging.getLogger("artisan")
    handler_types = [type(h).__name__ for h in artisan_logger.handlers]
    print(f"\nHandler types on artisan logger: {handler_types}")


def demo_idempotent() -> None:
    """Show that configure_logging is safe to call multiple times."""
    _reset_logging()
    _banner("Idempotent calls")

    from artisan.utils.logging import configure_logging

    configure_logging(suppress_noise=False)
    count_1 = len(logging.getLogger("artisan").handlers)
    print(f"After 1st call: {count_1} handler(s)")

    configure_logging(suppress_noise=False)
    count_2 = len(logging.getLogger("artisan").handlers)
    print(f"After 2nd call: {count_2} handler(s)")

    configure_logging(suppress_noise=False)
    count_3 = len(logging.getLogger("artisan").handlers)
    print(f"After 3rd call: {count_3} handler(s)")
    print("  (no duplicate handlers)")


def demo_tools_logger() -> None:
    """Show that artisan.tools inherits from the artisan parent logger."""
    _reset_logging()
    _banner("artisan.tools logger (external tool output)")

    from artisan.utils.logging import configure_logging

    configure_logging(level="DEBUG", suppress_noise=False)

    tools_logger = logging.getLogger("artisan.tools")
    artisan_logger = logging.getLogger("artisan")

    print(f"artisan.tools parent: {tools_logger.parent.name}")
    print(f"artisan.tools has own handlers: {bool(tools_logger.handlers)}")
    print(f"artisan.tools effective level: {logging.getLevelName(tools_logger.getEffectiveLevel())}")
    print()

    # Simulate what _run_with_streaming now does
    print("Simulated external tool output (via logger instead of print):")
    tools_logger.debug("Processing batch 1/3...")
    tools_logger.debug("Processing batch 2/3...")
    tools_logger.debug("Processing batch 3/3...")
    print("\n  (output routed through logger — respects level, format, file handler)")


def demo_auto_configure() -> None:
    """Show that PipelineManager auto-configures logging."""
    _reset_logging()
    _banner("Auto-configure via PipelineManager")

    artisan_logger = logging.getLogger("artisan")

    print(f"Before PipelineManager: handlers={len(artisan_logger.handlers)}, "
          f"level={logging.getLevelName(artisan_logger.level)}")
    print(f"PREFECT_LOGGING_LEVEL = {os.environ.get('PREFECT_LOGGING_LEVEL', '(not set)')}")

    print("\nPipelineManager.__init__() now calls configure_logging() automatically.")
    print("Users no longer need to call it manually.\n")

    # Simulate what __init__ does (without actually creating a pipeline)
    from artisan.utils.logging import configure_logging

    with tempfile.TemporaryDirectory() as tmpdir:
        logs_root = Path(tmpdir) / "logs"
        configure_logging(logs_root=logs_root)

    print(f"After PipelineManager: handlers={len(artisan_logger.handlers)}, "
          f"level={logging.getLevelName(artisan_logger.level)}")
    print(f"PREFECT_LOGGING_LEVEL = {os.environ.get('PREFECT_LOGGING_LEVEL', '(not set)')}")


def demo_failure_logs_path() -> None:
    """Show the new failure_logs_root path."""
    _banner("Failure logs path change")

    print("Before: <delta_root>/../failure_logs/")
    print("After:  <delta_root>/../logs/failures/")
    print()
    print("This groups all log-related directories under logs/:")
    print("  logs/")
    print("  +-- pipeline.log      (DEBUG file handler)")
    print("  +-- failures/          (worker failure logs)")


def main() -> None:
    demo_prefect_env_var()
    demo_info_vs_debug()
    demo_file_handler()
    demo_idempotent()
    demo_tools_logger()
    demo_auto_configure()
    demo_failure_logs_path()

    _banner("Summary")
    print("All logging redesign features demonstrated successfully.")
    print()
    print("Key changes:")
    print("  - PREFECT_LOGGING_LEVEL env var suppresses Prefect in child processes")
    print("  - PipelineManager auto-configures logging (no manual setup needed)")
    print("  - INFO level is clean: internal details demoted to DEBUG")
    print("  - DEBUG + logs_root enables file logging (RotatingFileHandler)")
    print("  - External tool output routed through artisan.tools logger")
    print("  - Failure logs moved to logs/failures/ for organization")


if __name__ == "__main__":
    main()
