use std::fmt;

/// Stable, machine-readable integrity error codes (spec §24).
///
/// Codes are never reused or renumbered. New codes are appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ErrorCode {
    /// `INT-001`: a constraint definition is invalid (e.g. unsupported key type).
    InvalidConstraint,
    /// `INT-002`: the referenced constraint does not exist.
    ConstraintNotFound,
    /// `INT-003`: a commit would produce a duplicate primary key.
    DuplicatePrimaryKey,
    /// `INT-004`: a commit would produce a duplicate unique key.
    DuplicateUniqueKey,
    /// `INT-005`: a child key has no matching parent key.
    ForeignKeyViolation,
    /// `INT-006`: a commit deletes a parent key that is still referenced.
    ReferencedRowDelete,
    /// `INT-007`: a NOT NULL (or primary key) column contains NULL.
    NotNullViolation,
    /// `INT-008`: a CHECK constraint failed (0.2+).
    CheckViolation,
    /// `INT-009`: the commit's base snapshot is stale.
    StaleBaseSnapshot,
    /// `INT-010`: the integrity domain is degraded; commits are refused.
    IndexDegraded,
    /// `INT-011`: an unresolved transaction blocks the domain.
    RecoveryRequired,
    /// `INT-012`: the commit contains a change the Plane cannot prove.
    UnsupportedCommitOperation,
    /// `INT-013`: an existing table violates a constraint being registered.
    OnboardingViolations,
    /// `INT-014`: inline validation would exceed the configured read budget.
    ValidationBudgetExceeded,
    /// `INT-015`: the certificate chain is broken (a writer bypassed the Plane).
    BypassDetected,
}

impl ErrorCode {
    /// Every code, in numeric order.
    pub const ALL: [ErrorCode; 15] = [
        ErrorCode::InvalidConstraint,
        ErrorCode::ConstraintNotFound,
        ErrorCode::DuplicatePrimaryKey,
        ErrorCode::DuplicateUniqueKey,
        ErrorCode::ForeignKeyViolation,
        ErrorCode::ReferencedRowDelete,
        ErrorCode::NotNullViolation,
        ErrorCode::CheckViolation,
        ErrorCode::StaleBaseSnapshot,
        ErrorCode::IndexDegraded,
        ErrorCode::RecoveryRequired,
        ErrorCode::UnsupportedCommitOperation,
        ErrorCode::OnboardingViolations,
        ErrorCode::ValidationBudgetExceeded,
        ErrorCode::BypassDetected,
    ];

    /// The numeric part of the code, e.g. `5` for `INT-005`.
    pub const fn number(self) -> u16 {
        match self {
            ErrorCode::InvalidConstraint => 1,
            ErrorCode::ConstraintNotFound => 2,
            ErrorCode::DuplicatePrimaryKey => 3,
            ErrorCode::DuplicateUniqueKey => 4,
            ErrorCode::ForeignKeyViolation => 5,
            ErrorCode::ReferencedRowDelete => 6,
            ErrorCode::NotNullViolation => 7,
            ErrorCode::CheckViolation => 8,
            ErrorCode::StaleBaseSnapshot => 9,
            ErrorCode::IndexDegraded => 10,
            ErrorCode::RecoveryRequired => 11,
            ErrorCode::UnsupportedCommitOperation => 12,
            ErrorCode::OnboardingViolations => 13,
            ErrorCode::ValidationBudgetExceeded => 14,
            ErrorCode::BypassDetected => 15,
        }
    }

    /// The code as it appears on the wire, e.g. `"INT-005"`.
    pub const fn code(self) -> &'static str {
        match self {
            ErrorCode::InvalidConstraint => "INT-001",
            ErrorCode::ConstraintNotFound => "INT-002",
            ErrorCode::DuplicatePrimaryKey => "INT-003",
            ErrorCode::DuplicateUniqueKey => "INT-004",
            ErrorCode::ForeignKeyViolation => "INT-005",
            ErrorCode::ReferencedRowDelete => "INT-006",
            ErrorCode::NotNullViolation => "INT-007",
            ErrorCode::CheckViolation => "INT-008",
            ErrorCode::StaleBaseSnapshot => "INT-009",
            ErrorCode::IndexDegraded => "INT-010",
            ErrorCode::RecoveryRequired => "INT-011",
            ErrorCode::UnsupportedCommitOperation => "INT-012",
            ErrorCode::OnboardingViolations => "INT-013",
            ErrorCode::ValidationBudgetExceeded => "INT-014",
            ErrorCode::BypassDetected => "INT-015",
        }
    }

    /// The symbolic name, e.g. `"FOREIGN_KEY_VIOLATION"`.
    pub const fn name(self) -> &'static str {
        match self {
            ErrorCode::InvalidConstraint => "INVALID_CONSTRAINT",
            ErrorCode::ConstraintNotFound => "CONSTRAINT_NOT_FOUND",
            ErrorCode::DuplicatePrimaryKey => "DUPLICATE_PRIMARY_KEY",
            ErrorCode::DuplicateUniqueKey => "DUPLICATE_UNIQUE_KEY",
            ErrorCode::ForeignKeyViolation => "FOREIGN_KEY_VIOLATION",
            ErrorCode::ReferencedRowDelete => "REFERENCED_ROW_DELETE",
            ErrorCode::NotNullViolation => "NOT_NULL_VIOLATION",
            ErrorCode::CheckViolation => "CHECK_VIOLATION",
            ErrorCode::StaleBaseSnapshot => "STALE_BASE_SNAPSHOT",
            ErrorCode::IndexDegraded => "INDEX_DEGRADED",
            ErrorCode::RecoveryRequired => "RECOVERY_REQUIRED",
            ErrorCode::UnsupportedCommitOperation => "UNSUPPORTED_COMMIT_OPERATION",
            ErrorCode::OnboardingViolations => "ONBOARDING_VIOLATIONS",
            ErrorCode::ValidationBudgetExceeded => "VALIDATION_BUDGET_EXCEEDED",
            ErrorCode::BypassDetected => "BYPASS_DETECTED",
        }
    }

    /// Parses a wire code such as `"INT-005"`.
    pub fn from_code(code: &str) -> Result<Self, UnknownErrorCode> {
        Self::ALL
            .into_iter()
            .find(|c| c.code() == code)
            .ok_or(UnknownErrorCode)
    }

    /// Parses a symbolic name such as `"FOREIGN_KEY_VIOLATION"`.
    pub fn from_name(name: &str) -> Result<Self, UnknownErrorCode> {
        Self::ALL
            .into_iter()
            .find(|c| c.name() == name)
            .ok_or(UnknownErrorCode)
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.code(), self.name())
    }
}

/// Returned when parsing a string that is not a known error code or name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownErrorCode;

impl fmt::Display for UnknownErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("unknown integrity error code")
    }
}

impl std::error::Error for UnknownErrorCode {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// The spec §24 table, verbatim. Changing this test means breaking a public contract.
    const SPEC_TABLE: [(&str, &str); 15] = [
        ("INT-001", "INVALID_CONSTRAINT"),
        ("INT-002", "CONSTRAINT_NOT_FOUND"),
        ("INT-003", "DUPLICATE_PRIMARY_KEY"),
        ("INT-004", "DUPLICATE_UNIQUE_KEY"),
        ("INT-005", "FOREIGN_KEY_VIOLATION"),
        ("INT-006", "REFERENCED_ROW_DELETE"),
        ("INT-007", "NOT_NULL_VIOLATION"),
        ("INT-008", "CHECK_VIOLATION"),
        ("INT-009", "STALE_BASE_SNAPSHOT"),
        ("INT-010", "INDEX_DEGRADED"),
        ("INT-011", "RECOVERY_REQUIRED"),
        ("INT-012", "UNSUPPORTED_COMMIT_OPERATION"),
        ("INT-013", "ONBOARDING_VIOLATIONS"),
        ("INT-014", "VALIDATION_BUDGET_EXCEEDED"),
        ("INT-015", "BYPASS_DETECTED"),
    ];

    #[test]
    fn codes_match_spec_table() {
        for (code, (wire, name)) in ErrorCode::ALL.into_iter().zip(SPEC_TABLE) {
            assert_eq!(code.code(), wire);
            assert_eq!(code.name(), name);
            assert_eq!(format!("INT-{:03}", code.number()), wire);
        }
    }

    #[test]
    fn codes_and_names_are_unique() {
        let codes: HashSet<_> = ErrorCode::ALL.iter().map(|c| c.code()).collect();
        let names: HashSet<_> = ErrorCode::ALL.iter().map(|c| c.name()).collect();
        assert_eq!(codes.len(), ErrorCode::ALL.len());
        assert_eq!(names.len(), ErrorCode::ALL.len());
    }

    #[test]
    fn parse_round_trips() {
        for c in ErrorCode::ALL {
            assert_eq!(ErrorCode::from_code(c.code()), Ok(c));
            assert_eq!(ErrorCode::from_name(c.name()), Ok(c));
        }
        assert_eq!(ErrorCode::from_code("INT-016"), Err(UnknownErrorCode));
        assert_eq!(ErrorCode::from_code("int-001"), Err(UnknownErrorCode));
        assert_eq!(ErrorCode::from_name("FOREIGN_KEY"), Err(UnknownErrorCode));
    }

    #[test]
    fn display_shows_code_and_name() {
        assert_eq!(
            ErrorCode::ForeignKeyViolation.to_string(),
            "INT-005 FOREIGN_KEY_VIOLATION"
        );
    }
}
