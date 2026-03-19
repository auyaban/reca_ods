def get_automation_attachment_analysis(*args, **kwargs):
    from app.automation.orchestrator import get_automation_attachment_analysis as _impl
    return _impl(*args, **kwargs)


def get_automation_gmail_preview(*args, **kwargs):
    from app.automation.orchestrator import get_automation_gmail_preview as _impl
    return _impl(*args, **kwargs)


def process_automation_email_preview(*args, **kwargs):
    from app.automation.orchestrator import process_automation_email_preview as _impl
    return _impl(*args, **kwargs)


def publish_automation_email_preview(*args, **kwargs):
    from app.automation.orchestrator import publish_automation_email_preview as _impl
    return _impl(*args, **kwargs)


def get_automation_staging_case(*args, **kwargs):
    from app.automation.orchestrator import get_automation_staging_case as _impl
    return _impl(*args, **kwargs)


def get_automation_staging_cases(*args, **kwargs):
    from app.automation.orchestrator import get_automation_staging_cases as _impl
    return _impl(*args, **kwargs)


def get_automation_test_status(*args, **kwargs):
    from app.automation.orchestrator import get_automation_test_status as _impl
    return _impl(*args, **kwargs)


def save_automation_staging_case(*args, **kwargs):
    from app.automation.orchestrator import save_automation_staging_case as _impl
    return _impl(*args, **kwargs)


def update_automation_staging_case(*args, **kwargs):
    from app.automation.orchestrator import update_automation_staging_case as _impl
    return _impl(*args, **kwargs)


def run_batch_eod_scan(*args, **kwargs):
    from app.automation.orchestrator import run_batch_eod_scan as _impl
    return _impl(*args, **kwargs)


def confirm_batch_eod_upload(*args, **kwargs):
    from app.automation.orchestrator import confirm_batch_eod_upload as _impl
    return _impl(*args, **kwargs)


__all__ = [
    "get_automation_test_status",
    "get_automation_gmail_preview",
    "process_automation_email_preview",
    "publish_automation_email_preview",
    "get_automation_attachment_analysis",
    "get_automation_staging_case",
    "get_automation_staging_cases",
    "save_automation_staging_case",
    "update_automation_staging_case",
    "run_batch_eod_scan",
    "confirm_batch_eod_upload",
]
