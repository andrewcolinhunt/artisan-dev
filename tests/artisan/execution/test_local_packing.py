"""Ownership transitions used by sequential packed worker units."""

from __future__ import annotations

import threading
import time

import pytest

from artisan.utils.command_supervisor import (
    CommandOwnershipError,
    CommandSupervisor,
    attached_worker_unit,
    check_command_scope,
    managed_invocation,
)


def test_attached_units_cover_joined_threads_and_restore_scope() -> None:
    owners = [CommandSupervisor.start(), CommandSupervisor.start()]
    observed = []
    try:
        for owner in owners:
            with attached_worker_unit(owner.attachment):
                with (
                    pytest.raises(CommandOwnershipError, match="already active"),
                    attached_worker_unit(owner.attachment),
                ):
                    pytest.fail("Nested packed unit entered")
                with managed_invocation():

                    def joined(selected_owner: CommandSupervisor = owner) -> None:
                        check_command_scope()
                        observed.append(selected_owner.attachment["secret"])

                    thread = threading.Thread(target=joined)
                    thread.start()
                    thread.join(timeout=5)
                    assert not thread.is_alive()
            assert owner.finish(time.monotonic() + 10).state == "closed"
        assert observed == [owner.attachment["secret"] for owner in owners]
        with managed_invocation():
            check_command_scope()
    finally:
        for owner in owners:
            owner.request_close(time.monotonic() + 10)
            owner.finish(time.monotonic() + 10)


def test_closed_unit_attachment_rejects_before_body() -> None:
    owner = CommandSupervisor.start()
    owner.request_close(time.monotonic() + 10)
    assert owner.finish(time.monotonic() + 10).state == "closed"
    with pytest.raises(CommandOwnershipError), attached_worker_unit(owner.attachment):
        pytest.fail("Closed unit reached live payload loading")
