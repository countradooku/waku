//! Credential scopes (RFC 0003 §6.1).

open_string_enum! {
    /// What a credential may do. `admin` grants everything; the local socket
    /// always receives it.
    pub enum Scope {
        Read = "read",
        Operate = "operate",
        FilesRead = "files:read",
        FilesWrite = "files:write",
        VcsWrite = "vcs:write",
        Terminal = "terminal",
        ProvidersManage = "providers:manage",
        SettingsWrite = "settings:write",
        DiagnosticsRead = "diagnostics:read",
        Admin = "admin",
    }
}

impl Scope {
    /// Whether a session granted `granted` may use something that needs
    /// `self`. An unknown required scope is granted only by `admin` or by
    /// itself.
    pub fn granted_by(&self, granted: &[Scope]) -> bool {
        granted
            .iter()
            .any(|scope| scope == self || *scope == Scope::Admin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_grants_everything_and_unknown_values_survive() {
        assert!(Scope::Terminal.granted_by(&[Scope::Admin]));
        assert!(Scope::Read.granted_by(&[Scope::Read, Scope::Operate]));
        assert!(!Scope::FilesWrite.granted_by(&[Scope::FilesRead]));
        let future = Scope::parse("mcp:invoke");
        assert_eq!(future, Scope::Unknown("mcp:invoke".into()));
        assert_eq!(future.as_str(), "mcp:invoke");
        assert!(future.granted_by(&[Scope::Admin]));
        assert_eq!(
            serde_json::to_string(&Scope::FilesRead).unwrap(),
            "\"files:read\""
        );
        assert_eq!(
            serde_json::from_str::<Scope>("\"vcs:write\"").unwrap(),
            Scope::VcsWrite
        );
    }
}
