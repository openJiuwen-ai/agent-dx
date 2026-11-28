//! Server-side authorization of an immutable target tenant.
use crate::{config::TenantType, session::{Role, SessionRecord}};

pub struct AuthorizedTenant {
    pub tenant_id: String,
    pub tenant_type: TenantType,
}

pub fn authorize_tenant(session: &SessionRecord, tenant_id: &str, tenant_type: TenantType) -> Result<AuthorizedTenant, &'static str> {
    if session.role == Role::TenantAdmin && session.tenant_id.as_deref() != Some(tenant_id) {
        return Err("tenant_forbidden");
    }
    Ok(AuthorizedTenant { tenant_id: tenant_id.into(), tenant_type })
}

pub fn require_digitalmate(target: &AuthorizedTenant) -> Result<(), &'static str> {
    if target.tenant_type == TenantType::Standard { return Err("feature_not_enabled"); }
    Ok(())
}
