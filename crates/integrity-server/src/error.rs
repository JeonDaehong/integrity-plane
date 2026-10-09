//! Integrity outcomes as HTTP responses (RFC 0003).

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use integrity_types::ErrorCode;
use serde_json::json;

/// A decision or failure of the Plane, before anything was forwarded upstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// Integrity code.
    pub code: ErrorCode,
    /// Human-readable detail (never key values unless the redaction policy allows).
    pub message: String,
    /// Structured detail returned by the integrity API (violation reports).
    pub report: Option<serde_json::Value>,
}

impl ApiError {
    /// An error with a message.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        let message = message.into();
        // Errors from lower layers display as "<code>: detail"; the body adds the code itself.
        let message = match message.strip_prefix(&format!("{code}: ")) {
            Some(detail) => detail.to_owned(),
            None => message,
        };
        Self {
            code,
            message,
            report: None,
        }
    }

    /// Attaches a structured report.
    pub fn with_report(mut self, report: serde_json::Value) -> Self {
        self.report = Some(report);
        self
    }

    /// The HTTP status of RFC 0003. Never 5xx: these are decisions the Plane made.
    pub fn status(&self) -> StatusCode {
        match self.code {
            ErrorCode::StaleBaseSnapshot
            | ErrorCode::RecoveryRequired
            | ErrorCode::StorageReadFailed => StatusCode::CONFLICT,
            ErrorCode::IndexDegraded | ErrorCode::BypassDetected => StatusCode::LOCKED,
            ErrorCode::ConstraintNotFound => StatusCode::NOT_FOUND,
            _ => StatusCode::BAD_REQUEST,
        }
    }

    /// The Iceberg `ErrorModel.type`.
    pub fn error_type(&self) -> &'static str {
        match self.status() {
            StatusCode::CONFLICT => "CommitFailedException",
            StatusCode::LOCKED => "IntegrityUnavailableException",
            StatusCode::NOT_FOUND => "NotFoundException",
            _ => "IntegrityViolationException",
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let mut body = json!({
            "error": {
                "message": format!("{} {}: {}", self.code.code(), self.code.name(), self.message),
                "type": self.error_type(),
                "code": status.as_u16(),
                "stack": [],
            }
        });
        if let Some(report) = self.report {
            body["integrity"] = report;
        }
        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc_0003_mapping() {
        let s = |code| ApiError::new(code, "").status().as_u16();
        for code in [
            ErrorCode::DuplicatePrimaryKey,
            ErrorCode::DuplicateUniqueKey,
            ErrorCode::ForeignKeyViolation,
            ErrorCode::ReferencedRowDelete,
            ErrorCode::NotNullViolation,
            ErrorCode::CheckViolation,
            ErrorCode::InvalidConstraint,
            ErrorCode::UnsupportedCommitOperation,
            ErrorCode::ValidationBudgetExceeded,
            ErrorCode::OnboardingViolations,
        ] {
            assert_eq!(s(code), 400, "{code}");
        }
        for code in [
            ErrorCode::StaleBaseSnapshot,
            ErrorCode::RecoveryRequired,
            ErrorCode::StorageReadFailed,
        ] {
            assert_eq!(s(code), 409, "{code}");
        }
        for code in [ErrorCode::IndexDegraded, ErrorCode::BypassDetected] {
            assert_eq!(s(code), 423, "{code}");
        }
        // No integrity code ever maps to a 5xx.
        for code in ErrorCode::ALL {
            assert!(s(code) < 500, "{code}");
        }
    }

    #[test]
    fn the_code_appears_once_in_the_message() {
        let e = ApiError::new(
            ErrorCode::StaleBaseSnapshot,
            format!("{}: requirement failed", ErrorCode::StaleBaseSnapshot),
        );
        assert_eq!(e.message, "requirement failed");
        let other = ApiError::new(ErrorCode::IndexDegraded, "INT-009 elsewhere");
        assert_eq!(other.message, "INT-009 elsewhere");
    }

    #[test]
    fn body_is_an_iceberg_error_model() {
        let r = ApiError::new(ErrorCode::ForeignKeyViolation, "fk_orders_customer");
        assert_eq!(r.error_type(), "IntegrityViolationException");
        assert_ne!(r.error_type(), "IllegalArgumentException");
    }
}
