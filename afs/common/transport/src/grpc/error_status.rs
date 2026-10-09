//! main 的 richer error 策略：google.rpc.Status -> Any -> AFS ErrorDetail。
//! 原生 tonic 错误可能没有业务详情，此时保留 gRPC 类别；不推断写操作是否发生。
use afs_error::{
    CLIENT_PROTOCOL_INVALID_ERROR_DETAIL, CLIENT_REMOTE_STATUS, Error, ErrorCode, ErrorKind,
};
use afs_protocol::error as pb;
use prost::Message;
use prost_types::Any;
use tonic::{Code, Status};
use tonic_types::pb::Status as RichStatus;
const TYPE_URL: &str = "type.googleapis.com/afs.error.v1.ErrorDetail";
pub fn error_to_status(error: Error) -> Status {
    let code = grpc_code(error.kind());
    let detail = pb::ErrorDetail {
        afs_code: error.code().raw(),
        kind: wire_kind(error.kind()) as i32,
        message: error.message().into(),
    };
    let rich = RichStatus {
        code: code as i32,
        message: error.message().into(),
        details: vec![Any {
            type_url: TYPE_URL.into(),
            value: detail.encode_to_vec(),
        }],
    };
    Status::with_details(code, error.message(), rich.encode_to_vec().into())
}
/// 业务返回只调用此函数，避免散落 Status::xxx 丢掉机器错误身份。
pub fn coded_status(code: ErrorCode, message: impl Into<String>) -> Status {
    error_to_status(Error::coded(code, message))
}
pub fn status_to_error(status: Status) -> Error {
    if status.details().is_empty() {
        return Error::new(
            CLIENT_REMOTE_STATUS,
            from_grpc_code(status.code()),
            status.message(),
        );
    }
    match decode(&status) {
        Ok(error) => error,
        Err(message) => Error::coded(CLIENT_PROTOCOL_INVALID_ERROR_DETAIL, message),
    }
}
fn decode(status: &Status) -> Result<Error, &'static str> {
    let rich = RichStatus::decode(status.details()).map_err(|_| "invalid google.rpc.Status")?;
    if rich.code != status.code() as i32 || rich.message != status.message() {
        return Err("inconsistent outer and inner gRPC status");
    }
    let mut details = rich.details.into_iter().filter(|d| d.type_url == TYPE_URL);
    let detail = details.next().ok_or("missing AFS ErrorDetail")?;
    if details.next().is_some() {
        return Err("duplicate AFS ErrorDetail");
    }
    let detail =
        pb::ErrorDetail::decode(detail.value.as_slice()).map_err(|_| "invalid AFS ErrorDetail")?;
    if detail.afs_code == 0 || detail.message != rich.message {
        return Err("invalid AFS error identity or message");
    }
    let kind = pb::ErrorKind::try_from(detail.kind).ok().map(native_kind);
    if let Some(kind) = kind
        && grpc_code(kind) != status.code()
    {
        return Err("AFS kind disagrees with gRPC code");
    }
    // 已知身份的分类稳定，防止 code 与同步伪造的 kind/status 一起漂移。
    // CLIENT_REMOTE_STATUS 是明确的例外：它表示无 AFS 详情的框架错误，分类来自 gRPC。
    if let Some(entry) = afs_error::ERROR_CATALOG
        .iter()
        .find(|e| e.code.raw() == detail.afs_code)
        && entry.code != CLIENT_REMOTE_STATUS
        && kind != Some(entry.kind)
    {
        return Err("known AFS code disagrees with catalog kind");
    }
    // Future kind cannot be interpreted, but precise numeric identity must survive.
    Ok(Error::new(
        ErrorCode::from_raw(detail.afs_code),
        kind.unwrap_or(ErrorKind::Unknown),
        detail.message,
    ))
}
pub fn grpc_code(kind: ErrorKind) -> Code {
    match kind {
        ErrorKind::Unknown => Code::Unknown,
        ErrorKind::Cancelled => Code::Cancelled,
        ErrorKind::OutOfRange => Code::OutOfRange,
        ErrorKind::InvalidArgument => Code::InvalidArgument,
        ErrorKind::NotFound => Code::NotFound,
        ErrorKind::AlreadyExists => Code::AlreadyExists,
        ErrorKind::PermissionDenied => Code::PermissionDenied,
        ErrorKind::ResourceExhausted => Code::ResourceExhausted,
        ErrorKind::FailedPrecondition => Code::FailedPrecondition,
        ErrorKind::Aborted => Code::Aborted,
        ErrorKind::Unimplemented => Code::Unimplemented,
        ErrorKind::Internal => Code::Internal,
        ErrorKind::Unavailable => Code::Unavailable,
        ErrorKind::DataLoss => Code::DataLoss,
        ErrorKind::Unauthenticated => Code::Unauthenticated,
        ErrorKind::DeadlineExceeded => Code::DeadlineExceeded,
    }
}
pub fn from_grpc_code(code: Code) -> ErrorKind {
    match code {
        Code::Unknown => ErrorKind::Unknown,
        Code::Cancelled => ErrorKind::Cancelled,
        Code::OutOfRange => ErrorKind::OutOfRange,
        Code::InvalidArgument => ErrorKind::InvalidArgument,
        Code::NotFound => ErrorKind::NotFound,
        Code::AlreadyExists => ErrorKind::AlreadyExists,
        Code::PermissionDenied => ErrorKind::PermissionDenied,
        Code::ResourceExhausted => ErrorKind::ResourceExhausted,
        Code::FailedPrecondition => ErrorKind::FailedPrecondition,
        Code::Aborted => ErrorKind::Aborted,
        Code::Unimplemented => ErrorKind::Unimplemented,
        Code::Internal => ErrorKind::Internal,
        Code::Unavailable => ErrorKind::Unavailable,
        Code::DataLoss => ErrorKind::DataLoss,
        Code::Unauthenticated => ErrorKind::Unauthenticated,
        Code::DeadlineExceeded => ErrorKind::DeadlineExceeded,
        Code::Ok => ErrorKind::Unknown,
    }
}
fn wire_kind(kind: ErrorKind) -> pb::ErrorKind {
    match kind {
        ErrorKind::Unknown => pb::ErrorKind::Unknown,
        ErrorKind::Cancelled => pb::ErrorKind::Cancelled,
        ErrorKind::OutOfRange => pb::ErrorKind::OutOfRange,
        ErrorKind::InvalidArgument => pb::ErrorKind::InvalidArgument,
        ErrorKind::NotFound => pb::ErrorKind::NotFound,
        ErrorKind::AlreadyExists => pb::ErrorKind::AlreadyExists,
        ErrorKind::PermissionDenied => pb::ErrorKind::PermissionDenied,
        ErrorKind::ResourceExhausted => pb::ErrorKind::ResourceExhausted,
        ErrorKind::FailedPrecondition => pb::ErrorKind::FailedPrecondition,
        ErrorKind::Aborted => pb::ErrorKind::Aborted,
        ErrorKind::Unimplemented => pb::ErrorKind::Unimplemented,
        ErrorKind::Internal => pb::ErrorKind::Internal,
        ErrorKind::Unavailable => pb::ErrorKind::Unavailable,
        ErrorKind::DataLoss => pb::ErrorKind::DataLoss,
        ErrorKind::Unauthenticated => pb::ErrorKind::Unauthenticated,
        ErrorKind::DeadlineExceeded => pb::ErrorKind::DeadlineExceeded,
    }
}
fn native_kind(kind: pb::ErrorKind) -> ErrorKind {
    match kind {
        pb::ErrorKind::Unknown => ErrorKind::Unknown,
        pb::ErrorKind::Cancelled => ErrorKind::Cancelled,
        pb::ErrorKind::OutOfRange => ErrorKind::OutOfRange,
        pb::ErrorKind::InvalidArgument => ErrorKind::InvalidArgument,
        pb::ErrorKind::NotFound => ErrorKind::NotFound,
        pb::ErrorKind::AlreadyExists => ErrorKind::AlreadyExists,
        pb::ErrorKind::PermissionDenied => ErrorKind::PermissionDenied,
        pb::ErrorKind::ResourceExhausted => ErrorKind::ResourceExhausted,
        pb::ErrorKind::FailedPrecondition => ErrorKind::FailedPrecondition,
        pb::ErrorKind::Aborted => ErrorKind::Aborted,
        pb::ErrorKind::Unimplemented => ErrorKind::Unimplemented,
        pb::ErrorKind::Internal => ErrorKind::Internal,
        pb::ErrorKind::Unavailable => ErrorKind::Unavailable,
        pb::ErrorKind::DataLoss => ErrorKind::DataLoss,
        pb::ErrorKind::Unauthenticated => ErrorKind::Unauthenticated,
        pb::ErrorKind::DeadlineExceeded => ErrorKind::DeadlineExceeded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use afs_error::*;
    #[test]
    fn every_registered_error_roundtrips_in_standard_google_envelope() {
        for definition in ERROR_CATALOG {
            let error = Error::coded(definition.code, "diagnostic text");
            let status = error_to_status(error.clone());
            let rich = RichStatus::decode(status.details()).unwrap();
            assert_eq!(rich.code, status.code() as i32);
            assert_eq!(rich.details[0].type_url, TYPE_URL);
            assert_eq!(status_to_error(status), error);
        }
    }
    #[test]
    fn future_code_and_future_kind_survive_without_guessing() {
        let error = Error::new(
            ErrorCode::from_raw(0x7f010001),
            ErrorKind::Unavailable,
            "new peer",
        );
        assert_eq!(status_to_error(error_to_status(error.clone())), error);
        let status = error_to_status(error);
        let mut rich = RichStatus::decode(status.details()).unwrap();
        let mut detail = pb::ErrorDetail::decode(rich.details[0].value.as_slice()).unwrap();
        detail.kind = 999;
        rich.details[0].value = detail.encode_to_vec();
        let decoded = status_to_error(Status::with_details(
            status.code(),
            status.message(),
            rich.encode_to_vec().into(),
        ));
        assert_eq!(decoded.code().raw(), 0x7f010001);
        assert_eq!(decoded.kind(), ErrorKind::Unknown);
    }
    #[test]
    fn known_code_cannot_change_kind_even_with_matching_outer_status() {
        let error = Error::new(
            NODE_STORAGE_NOT_FOUND,
            ErrorKind::PermissionDenied,
            "forged",
        );
        assert_eq!(
            status_to_error(error_to_status(error)).code(),
            CLIENT_PROTOCOL_INVALID_ERROR_DETAIL
        );
        let status = error_to_status(Error::coded(NODE_STORAGE_NOT_FOUND, "future kind"));
        let mut rich = RichStatus::decode(status.details()).unwrap();
        let mut detail = pb::ErrorDetail::decode(rich.details[0].value.as_slice()).unwrap();
        detail.kind = 999;
        rich.details[0].value = detail.encode_to_vec();
        assert_eq!(
            status_to_error(Status::with_details(
                status.code(),
                status.message(),
                rich.encode_to_vec().into()
            ))
            .code(),
            CLIENT_PROTOCOL_INVALID_ERROR_DETAIL
        );
        // 中间节点转发一个没有业务详情的上游框架错误，不能丢掉原始分类。
        let fallback = status_to_error(Status::unavailable("upstream disconnected"));
        assert_eq!(status_to_error(error_to_status(fallback.clone())), fallback);
    }
    #[test]
    fn framework_status_without_details_retains_category() {
        for (status, kind) in [
            (
                Status::unavailable("connection lost"),
                ErrorKind::Unavailable,
            ),
            (
                Status::deadline_exceeded("deadline"),
                ErrorKind::DeadlineExceeded,
            ),
            (Status::cancelled("cancelled"), ErrorKind::Cancelled),
        ] {
            let error = status_to_error(status);
            assert_eq!(error.code(), CLIENT_REMOTE_STATUS);
            assert_eq!(error.kind(), kind);
        }
    }
    #[test]
    fn malformed_duplicate_and_inconsistent_details_fail_closed() {
        let good = error_to_status(Error::coded(NODE_STORAGE_NOT_FOUND, "absent"));
        let rich = RichStatus::decode(good.details()).unwrap();
        let mut bad_cases = vec![vec![0xff]];
        let mut changed = rich.clone();
        changed.code = Code::PermissionDenied as i32;
        bad_cases.push(changed.encode_to_vec());
        let mut changed = rich.clone();
        changed.message = "different".into();
        bad_cases.push(changed.encode_to_vec());
        let mut changed = rich.clone();
        changed.details.push(changed.details[0].clone());
        bad_cases.push(changed.encode_to_vec());
        let mut changed = rich.clone();
        changed.details[0].type_url = "type.googleapis.com/other.Error".into();
        bad_cases.push(changed.encode_to_vec());
        for (code, kind, msg) in [
            (0, pb::ErrorKind::NotFound as i32, "absent"),
            (
                NODE_STORAGE_NOT_FOUND.raw(),
                pb::ErrorKind::PermissionDenied as i32,
                "absent",
            ),
            (
                NODE_STORAGE_NOT_FOUND.raw(),
                pb::ErrorKind::NotFound as i32,
                "different",
            ),
        ] {
            let mut changed = rich.clone();
            changed.details[0].value = pb::ErrorDetail {
                afs_code: code,
                kind,
                message: msg.into(),
            }
            .encode_to_vec();
            bad_cases.push(changed.encode_to_vec());
        }
        for bytes in bad_cases {
            let error = status_to_error(Status::with_details(
                good.code(),
                good.message(),
                bytes.into(),
            ));
            assert_eq!(error.code(), CLIENT_PROTOCOL_INVALID_ERROR_DETAIL);
        }
    }
}
