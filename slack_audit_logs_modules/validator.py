from datetime import UTC, datetime

from sekoia_automation.account_validator import AccountValidator

from slack_audit_logs_modules.client import AuditLogsClient
from slack_audit_logs_modules.errors import AuthenticationError, PlanError, SlackAuditLogsError

PROBE_WINDOW_SECONDS = 300


class SlackAuditLogsAccountValidator(AccountValidator):
    """Checks the Slack token with one cheap authenticated call."""

    def validate(self) -> bool:
        latest = int(datetime.now(UTC).timestamp())
        oldest = latest - PROBE_WINDOW_SECONDS

        try:
            # Constructed inside the try on purpose: a failure here (bad base_url, adapter mount)
            # would otherwise escape validate(), skip send_results(), and leave the console with
            # no verdict — the same defect the catch-all below exists to prevent.
            client = AuditLogsClient(
                base_url=self.module.configuration.base_url,
                token=self.module.configuration.token,
            )
            next(client.iter_pages(oldest=oldest, latest=latest, limit=1), None)
        except AuthenticationError as error:
            self.error(
                f"Slack rejected the token ({error}). Check that it carries auditlogs:read and "
                "that the app is installed on the Enterprise organization, not on a workspace."
            )
            return False
        except PlanError as error:
            self.error(
                f"This Slack organization cannot use the Audit Logs API ({error}). "
                "The API requires Enterprise Grid; the token itself may be fine."
            )
            return False
        except SlackAuditLogsError as error:
            self.error(f"The call to the Slack Audit Logs API failed ({error}). The token may still be valid.")
            return False
        except Exception as error:  # noqa: BLE001
            # The SDK's Module.run() wraps execute() in try/finally only, with no except: anything
            # raised here would skip send_results() and leave the console with no verdict at all.
            self.error(f"Unexpected failure while validating the Slack credentials: {error}")
            return False

        return True
