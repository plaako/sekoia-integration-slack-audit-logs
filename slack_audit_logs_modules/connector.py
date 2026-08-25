import time
from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import cached_property
from threading import Lock
from typing import Any

import orjson
from pydantic import Field
from sekoia_automation.checkpoint import CheckpointTimestamp, TimeUnit
from sekoia_automation.connector import Connector, DefaultConnectorConfiguration
from sekoia_automation.constants import EVENT_BYTES_MAX_SIZE
from sekoia_automation.storage import PersistentJSON

from slack_audit_logs_modules import SlackAuditLogsModule
from slack_audit_logs_modules.client import AuditLogsClient
from slack_audit_logs_modules.errors import AuthenticationError, PlanError, SlackAuditLogsError

# What iterate() hands the SDK: the serialised events, and the newest date among them.
Batch = tuple[list[str], datetime | None]


class SlackAuditLogsConnectorConfiguration(DefaultConnectorConfiguration):
    # Bounded because a typo in these console fields would otherwise be a silent production incident.
    # `timebuffer` at 0 would commit `latest = now`, leaving anything Slack indexes afterwards behind
    # the watermark for good.
    frequency: int = Field(default=60, ge=10, le=3600)
    limit: int = Field(default=1000, ge=1, le=9999)
    ratelimit_per_minute: int = Field(default=30, ge=1, le=50)
    timebuffer: int = Field(default=60, ge=1, le=3600)
    lookback_seconds: int = Field(default=3600, ge=60)


@dataclass
class WindowProgress:
    """What earlier cycles left behind for the window starting at a given second."""

    # The end this window was opened with, frozen so a resumed window is read to the same bound.
    window_end: int | None = None
    # Slack's pagination cursor: where the next page of this window starts.
    cursor: str = ""
    # Ids already forwarded for this window, oldest first, so a re-read does not push them again.
    pushed_ids: list[str] = field(default_factory=list)
    # True once _trim dropped ids, so a re-read may duplicate a few events.
    truncated: bool = False
    # Set by _read when Slack reports no more pages. Never persisted: it describes this cycle only.
    drained: bool = False


class SlackAuditLogsConnector(Connector):
    """Forwards Slack Enterprise Grid audit events to a Sekoia intake."""

    name = "Slack Audit Logs"
    description = "Collect audit events from the Slack Audit Logs API"

    module: SlackAuditLogsModule
    configuration: SlackAuditLogsConnectorConfiguration

    # Slack returns entries newest first and offers no sort parameter, so the backlog is read as
    # bounded windows walked forward: progress stays a single timestamp, and an interrupted cycle
    # resumes the window it left instead of stepping over it.
    SUB_WINDOW_SECONDS = 3600
    # The ledger only has to cover what a re-read hands back before it reaches new ground.
    LEDGER_PAGES = 2

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Written from the SDK's worker threads, read once they have all finished.
        self._chunk_lock = Lock()
        self._failed_chunks = 0

    @property
    def frequency(self) -> int:
        return self.configuration.frequency

    @cached_property
    def client(self) -> AuditLogsClient:
        return AuditLogsClient(
            base_url=self.module.configuration.base_url,
            token=self.module.configuration.token,
            per_minute=self.configuration.ratelimit_per_minute,
        )

    @cached_property
    def checkpoint(self) -> CheckpointTimestamp:
        return CheckpointTimestamp(
            path=self._data_path,
            time_unit=TimeUnit.SECOND,
            start_at=timedelta(seconds=self.configuration.lookback_seconds),
            # The SDK default clamps a watermark older than 30 days, silently skipping everything a
            # longer outage spans. Slack's own retention is the only bound we accept.
            ignore_older_than=None,
        )

    @cached_property
    def pending(self) -> PersistentJSON:
        # A separate file on purpose: PersistentJSON caches its dict and rewrites the whole file, so a
        # second instance on context.json would clobber CheckpointTimestamp's writes.
        return PersistentJSON("pending.json", self._data_path)

    def next_run(self) -> None:
        # The SDK skips its own pause whenever a cycle forwarded any event, so an active organization
        # would be polled flat out at `ratelimit_per_minute` - most of Slack's org-wide Tier-3 budget,
        # which every other app in the customer's org would feel as 429s.
        started = time.time()
        super().next_run()

        remaining = self.frequency - (time.time() - started)
        if remaining > 0:
            time.sleep(remaining)

    def _send_chunk(
        self, batch_api: str, chunk_index: int, chunk: list[Any], collect_ids: dict[int, list[str]]
    ) -> None:
        # In a `finally` because super() does not always return: its own failure handler calls
        # self.log(level="error"), a synchronous POST that re-raises when the platform API is
        # unreachable too - the very outage that broke the intake POST. That exception is swallowed by
        # the SDK's wait_futures, so counting on a normal return would miss the case entirely.
        try:
            super()._send_chunk(batch_api, chunk_index, chunk, collect_ids)
        finally:
            # The SDK inserts the key only once the POST succeeded, so its absence means exactly
            # "this chunk did not reach the intake".
            if chunk_index not in collect_ids:
                with self._chunk_lock:
                    self._failed_chunks += 1

    def push_events_to_intakes(self, events: list[str], sync: bool = False) -> list[str]:
        """Forward a batch, and refuse to let a forward that did not happen pass for one that did.

        The SDK logs a chunk it could not send, returns only the ids that landed, and next_run()
        discards even that - so a partly delivered batch looks exactly like a complete one. Raising
        keeps the window uncommitted, so the next cycle reads it again from the stored cursor.
        """
        self._failed_chunks = 0

        pushed = super().push_events_to_intakes(events, sync)

        if self._failed_chunks:
            self.log(
                message=(
                    f"{self._failed_chunks} of this batch's chunks did not reach the intake. Nothing is "
                    "recorded and the window stays uncommitted, so this page is read again next cycle "
                    "from the cursor in pending.json."
                ),
                level="warning",
            )

            raise SlackAuditLogsError(
                f"{self._failed_chunks} of this batch's chunks did not reach the intake "
                f"({len(events)} events were being forwarded)"
            )

        return pushed

    def _resume(self, window_start: int) -> WindowProgress:
        """What is stored for the window starting at `window_start`, or a blank slate for a new one."""
        with self.pending as cache:
            # Keyed on the window's start, which comes from the committed watermark and so survives a
            # restart.
            if cache.get("window_start") != window_start:
                return WindowProgress()

            return WindowProgress(
                window_end=cache.get("window_end"),
                cursor=cache.get("cursor") or "",
                pushed_ids=list(cache.get("pushed_ids") or []),
                truncated=bool(cache.get("truncated")),
            )

    def _remember(self, window_start: int, window_end: int, cursor: str, progress: WindowProgress) -> None:
        with self.pending as cache:
            cache["window_start"] = window_start
            cache["window_end"] = window_end
            cache["cursor"] = cursor
            cache["pushed_ids"] = list(progress.pushed_ids)
            cache["truncated"] = progress.truncated

    def _forget(self) -> None:
        # Writes the keys empty rather than clearing the dict: PersistentJSON.load() re-reads the file
        # whenever its cache is falsy, so an emptied dict costs a disk read on every later cycle.
        with self.pending as cache:
            cache["window_start"] = None
            cache["window_end"] = None
            cache["cursor"] = ""
            cache["pushed_ids"] = []
            cache["truncated"] = False

    def _trim(self, progress: WindowProgress) -> None:
        """Keep only the newest ids.

        Unbounded, the ledger would reach MAX_PAGES x limit ids and be rewritten whole after every
        page - megabytes of writes per cycle.
        """
        bound = self.LEDGER_PAGES * self.configuration.limit

        if len(progress.pushed_ids) > bound:
            progress.pushed_ids = progress.pushed_ids[-bound:]
            progress.truncated = True

    def iterate(self) -> Generator[Batch, None, None]:
        try:
            # Anything younger than the buffer is left for a later cycle, so an event Slack indexes
            # late is still ahead of the window boundary instead of behind the watermark.
            settled_until = int(datetime.now(UTC).timestamp()) - self.configuration.timebuffer
            # Slack's `oldest` is inclusive: +1 second means an event is never delivered twice.
            # Reading the watermark writes context.json, so it belongs inside this try.
            oldest = self.checkpoint.offset + 1

            while oldest <= settled_until:
                progress = self._resume(oldest)

                if progress.window_end is not None and progress.window_end > settled_until:
                    # A backwards clock step can leave a frozen end ahead of the buffer. Reading to it
                    # would commit past the buffer and lose whatever Slack indexes in between.
                    self.log(
                        message=(
                            f"The window in flight ends at {progress.window_end}, after the settled "
                            f"boundary {settled_until}: the clock has most likely stepped backwards. "
                            "Waiting for it to catch up."
                        ),
                        level="warning",
                    )
                    return

                # An unfinished window keeps the end it was opened with instead of stretching towards
                # the present, so a stored cursor stays paired with the window it was issued for.
                latest = (
                    progress.window_end
                    if progress.window_end is not None
                    else min(oldest + self.SUB_WINDOW_SECONDS - 1, settled_until)
                )

                yield from self._drain(oldest, latest, progress)

                if not progress.drained:
                    # The page budget ran out with a cursor still pending. Not committing is what makes
                    # that harmless: the stored cursor lets the next cycle carry on inside this window
                    # rather than skipping it or reading it from the start.
                    return

                # Commit the end the window was actually read to - the frozen one when resuming - and
                # drop the progress recorded inside it.
                self.checkpoint.offset = latest
                self._forget()

                oldest = latest + 1
        except (AuthenticationError, PlanError) as error:
            # Neither retrying nor waiting fixes these: a human must act on the token or the plan.
            self.log(
                message=(
                    f"Slack refused the collection: {error}. Check that the token carries "
                    "auditlogs:read, that the app is installed on the Enterprise organization "
                    "(not a workspace), and that the organization is on Enterprise Grid."
                ),
                level="critical",
            )
        except OSError as error:
            # In the image `data_path` falls back to /symphony_data, which nothing creates. Every state
            # access writes - PersistentJSON dumps on exit even when only read - so an unwritable path
            # can fail at the watermark, at _resume, or at _remember after the events were pushed. Only
            # a handler outside all of them catches every ordering.
            self.log(
                message=(
                    f"Cannot read or record the collection state under {self._data_path} ({error}). "
                    "Events already forwarded will be sent again on every cycle until that path is "
                    "writable - fix the volume mount before the intake fills up."
                ),
                level="critical",
            )
            raise

    def _drain(self, oldest: int, latest: int, progress: WindowProgress) -> Generator[Batch, None, None]:
        """Read one window, carrying on from any stored cursor."""
        try:
            yield from self._read(oldest, latest, progress.cursor, progress)
        except (AuthenticationError, PlanError):
            # Not the cursor's fault, and a retry would spend a request to fail the same way.
            raise
        except SlackAuditLogsError as error:
            if not progress.cursor:
                raise

            # Slack rejects a cursor it no longer recognises, so read the window from its start again.
            # The ledger - keyed on that unchanged start - holds back what an earlier cycle pushed.
            # Clearing the stored cursor first means a second failure does not present it again.
            #
            # This path is not optional: Splunk's TA re-presents a failing cursor on every later run
            # with no branch that clears it, so a cursor Slack stops recognising stalls that input for
            # good - and a window that never drains is a watermark that never advances.
            self.log(
                message=f"Slack rejected the stored cursor ({error}); re-reading the window from its start.",
                level="warning",
            )
            if progress.truncated:
                self.log(
                    message=(
                        "This window's ledger had been trimmed, so this re-read may deliver its "
                        "earliest events to the intake a second time."
                    ),
                    level="warning",
                )

            self._remember(oldest, latest, "", progress)
            yield from self._read(oldest, latest, "", progress)

    def _read(
        self, oldest: int, latest: int, cursor: str, progress: WindowProgress
    ) -> Generator[Batch, None, None]:
        """Walk the window's pages from `cursor`, forwarding each one."""
        for entries, next_cursor in self.client.iter_pages(
            oldest=oldest, latest=latest, limit=self.configuration.limit, cursor=cursor
        ):
            already_pushed = set(progress.pushed_ids)
            fresh = [
                event
                for event in entries
                # An entry with no id is never held back - see _identifier.
                if (identifier := self._identifier(event)) is None or identifier not in already_pushed
            ]

            if fresh:
                serialised = [orjson.dumps(event).decode("utf-8") for event in fresh]

                self._warn_about_missing_ids(fresh)
                self._report_oversized(fresh, serialised)

                yield serialised, self._newest_date(fresh)

                # Reached only once the SDK has pushed the batch above, so neither the cursor nor the
                # ledger is ever recorded ahead of what has actually been forwarded.
                progress.pushed_ids.extend(
                    identifier for event in fresh if (identifier := self._identifier(event)) is not None
                )
                self._trim(progress)

            # `latest` goes in too, so a resumed window is read to the same end it was opened with.
            self._remember(oldest, latest, next_cursor, progress)

            if not next_cursor:
                progress.drained = True
                return

    def _report_oversized(self, entries: list[dict[str, Any]], serialised: list[str]) -> None:
        """Name the events the platform will discard for their size, because nothing else will.

        The SDK drops them before a chunk exists, so no failure is countable and holding the window
        back would stall it for ever. Saying exactly which audit events never reached the SIEM is the
        only honest handling left; the SDK logs a bare count at info level.
        """
        oversized = [
            (self._identifier(entry) or "<no id>", len(payload))
            for entry, payload in zip(entries, serialised)
            if len(payload) > EVENT_BYTES_MAX_SIZE
        ]
        if not oversized:
            return

        self.log(
            message=(
                f"{len(oversized)} audit event(s) exceed the platform's per-event limit of "
                f"{EVENT_BYTES_MAX_SIZE} bytes and are discarded before any request is made, so they "
                "will never reach the intake and cannot be recovered by a retry: "
                + ", ".join(f"id {identifier} ({size} bytes)" for identifier, size in oversized)
            ),
            level="critical",
        )

    def _warn_about_missing_ids(self, entries: list[dict[str, Any]]) -> None:
        without_id = sum(1 for entry in entries if self._identifier(entry) is None)
        if not without_id:
            return

        self.log(
            message=(
                f"Forwarded {without_id} of {len(entries)} entries with no id. Nothing can hold those "
                "back, so a re-read of this window may deliver them to the intake twice - a duplicate "
                "is preferred to dropping one for good."
            ),
            level="warning",
        )

    @staticmethod
    def _identifier(event: dict[str, Any]) -> str | None:
        """The entry's Slack id, or None when it carries none.

        An entry with no id is never suppressed: nothing tells a re-delivery apart from a second,
        genuinely distinct entry, and the requirement ranks a miss above a duplicate.
        """
        identifier = event.get("id")

        return None if identifier is None else str(identifier)

    @staticmethod
    def _timestamp_of(event: dict[str, Any]) -> int | None:
        """The event's creation time, or None when the entry carries nothing usable.

        Slack stamps every audit entry, and both Splunk's TA and Sentinel's DCR read `date_create`
        unguarded - so this tolerates its absence only to keep a malformed page from crashing the
        cycle. Nothing reports it: the entry is still forwarded, and the only casualty is its
        contribution to the event-lag figure.
        """
        try:
            return int(event["date_create"])
        except (KeyError, TypeError, ValueError):
            return None

    def _newest_date(self, entries: list[dict[str, Any]]) -> datetime | None:
        """The newest usable creation time in the batch, or None when no entry carries one."""
        stamps = [stamp for stamp in (self._timestamp_of(entry) for entry in entries) if stamp is not None]
        if not stamps:
            return None

        return datetime.fromtimestamp(max(stamps), tz=UTC).replace(tzinfo=None)
