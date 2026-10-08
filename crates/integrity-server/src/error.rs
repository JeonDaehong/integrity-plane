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
}

impl ApiError {
    /// An error with a message.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
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
        let body = json!({
            "error": {
                "message": format!("{} {}: {}", self.code.code(), self.code.name(), self.message),
                "type": self.error_type(),
                "code": status.as_u16(),
                "stack": [],
            }
        });
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
    fn body_is_an_iceberg_error_model() {
        let r = ApiError::new(ErrorCode::ForeignKeyViolation, "fk_orders_customer");
        assert_eq!(r.error_type(), "IntegrityViolationException");
        assert_ne!(r.error_type(), "IllegalArgumentException");
    }
}
