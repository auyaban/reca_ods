from __future__ import annotations

from app.automation.models import DecisionSuggestion, ExtractedActaData, GmailMessageRef


class AutomationStagingRepository:
    """Contract for storing reviewable automation cases before publication."""

    def save_case(
        self,
        *,
        message: GmailMessageRef,
        extracted: ExtractedActaData,
        suggestion: DecisionSuggestion,
    ) -> str:
        raise NotImplementedError("Pendiente implementar staging de automatizacion.")
