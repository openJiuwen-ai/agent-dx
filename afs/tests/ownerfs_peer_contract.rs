#![cfg(feature = "ownerfs")]

use std::{
    ffi::OsStr,
    os::unix::fs::MetadataExt,
    sync::{Arc, Mutex},
    time::Duration,
};

#[cfg(feature = "rdma")]
use afs::node::rpc::{
    control::{PeerSessionIdentity, RDMA_HANDSHAKE_VERSION, RdmaSessionRegistry},
    data::{OwnerFilesHandler, OwnerFilesService},
    peer::{DataMode as ClientDataMode, owner_files_client_from_channel_with_runtime},
};
use afs::node::{
    rpc::{
        data::{
            MtlsPeerAuthenticator, PeerAuthenticator, make_owner_files_handler,
            make_owner_files_server_with_handler,
        },
        peer::owner_files_client_from_channel,
    },
    storage::LocalFs,
    vfs::{
        Backend,
        ownerfs::{
            OwnerFs,
            catalog::LocalRootRecord,
            remote::RemoteFiles,
            root::{
                PreparedRoot, RootGrant, RootId, RootLocation, RootManager, RootMeta,
                RootReservation, RootRight, root_id_from_name,
            },
        },
        types::{BackendInode, RenameFlags, RequestContext},
    },
};
#[cfg(feature = "rdma")]
use afs_protocol::node_control::{
    NegotiateDataRequest, OwnerCloseDataRequest, OwnerNegotiateDataRequest,
    node_control_client::NodeControlClient,
};
use afs_protocol::node_data::{
    DataPlane, DataTransfer, FileIdentity, OwnerCaller, OwnerCreateRequest, OwnerGetAttrRequest,
    OwnerLookupRequest, OwnerOpenRequest, OwnerReadRequest, OwnerReleaseRequest,
    OwnerRenameRequest, OwnerRmdirRequest, OwnerUnlinkRequest, OwnerWriteRequest, RootAccess,
    owner_files_client::OwnerFilesClient,
};
#[cfg(feature = "rdma")]
use afs_protocol::node_data::{
    OwnerFileAttr, OwnerFileKind, OwnerFilesystemCapacity, OwnerFsyncRequest, OwnerHandle,
    OwnerOpenReply, OwnerReadReply, OwnerStatFsReply, OwnerStatFsRequest,
    owner_files_server::OwnerFilesServer,
};
#[cfg(feature = "rdma")]
use afs_transport::rdma::RdmaEndpoint;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    metadata::MetadataMap,
    transport::{
        Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig,
    },
};

const CA_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDDTCCAfWgAwIBAgIUG8Y1Uz4fkfWEstXKUj2ZSXzPO9IwDQYJKoZIhvcNAQEL
BQAwFjEUMBIGA1UEAwwLQUZTIFRlc3QgQ0EwHhcNMjYwOTI2MTMxNjA1WhcNMzYw
OTIzMTMxNjA1WjAWMRQwEgYDVQQDDAtBRlMgVGVzdCBDQTCCASIwDQYJKoZIhvcN
AQEBBQADggEPADCCAQoCggEBAJO1iyYRT/xJhc6WBY+1BZ3EEGwmxEgxEVzBr9Qj
USOHEphRpah7xDTLYRW7m8/+ks0RrNP3EbHTrBaWrIhtn6E9nIRTXjFPCTGxURZH
eDK9FnJyA7f39wnn+ntA9fkcCJbSoziTFK/awb5m+IM7i9yjSd+oNal5a3gX4CGU
0VzF//hWXCobKmNsisXn3Eq68p/Zk2ZBlnhcFcfWlqwpfpYLPI5vg2mfcB7/6fBj
uKLnEOQJ63rRAzVJnPJ+uvBcqVuhNcOWsvfKm++lg9zUcWxNj0NUcaoN+z/KVoww
vXNNMsf/qeoaKbD80dgA/7z58JzdCIcvj8HiDujS3fhYDEsCAwEAAaNTMFEwHQYD
VR0OBBYEFBGuiyvIVkf0gmmkYjM/EHUlIiMtMB8GA1UdIwQYMBaAFBGuiyvIVkf0
gmmkYjM/EHUlIiMtMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEB
ACxvpxjyCec9PkiEi3FEft7tYiDvCSIr/+8mneilcbPl2NBzdiTdOMnjei7dWwPh
pl9N+F29eB0lGn/OXTITbhj9VfFnvIytUHRLblPK48Lk9h346LBiw5kbxbDohZzy
vQnbKVj37QVbCDgyImtaNL6aqhTkCaktCDE5kkX8S4YQl2JYRElCEYkyPDrpGYRW
kCF+YI1RCdDvrlUY65w2sKK+vbbLBTT2B85NFzjnUBgTJcCcCLQjr77dmz1Wo1OY
H3fYD9LBiPmqt+qXwVTnc9SjoQUz/WXQ9MAuI6NZNc+oDfRkKv0WHPFnuUmqsqRl
21sgpGG7yJpiOKdZgErfCUU=
-----END CERTIFICATE-----"#;

const SERVER_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDKzCCAhOgAwIBAgIUExeNy5Q72Lhe/lXf4wFtZYqxdQ0wDQYJKoZIhvcNAQEL
BQAwFjEUMBIGA1UEAwwLQUZTIFRlc3QgQ0EwHhcNMjYwOTI2MTMxNjA1WhcNMzYw
OTIzMTMxNjA1WjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQC85iyEx46fVzhAcGQg8QKxShbGFaQLycgfY78E4/49
qvosj1p0H9kMuaCvs3asSq6S8T/dgnxAxltjAY08DphAr+26dMW7f7FBH/VBej8o
YCgyamjAvh1v2MK65tawS1phIpfGEW91qZhbmy9LqxNH7/2rkKvxp4it4VmFCm6G
DcnfygUTrvDveaMMIOlEi93FM5KrJJRlXJwdiKGYDZQH9BaQPVL0BiCE53PMDuDJ
sZH/Na+iSuLzpGa8iVFitfQ85UqIEP/Z2OHES/rODGC9w3dLYISby63lV3XloAUV
Dciv5KkWol0y+ZcIQNNNXhO/gBE892zSUjw6EeI5yJPhAgMBAAGjczBxMBoGA1Ud
EQQTMBGCCWxvY2FsaG9zdIcEfwAAATATBgNVHSUEDDAKBggrBgEFBQcDATAdBgNV
HQ4EFgQUygnBDdtLqm09NySOQhkXhV4dXTowHwYDVR0jBBgwFoAUEa6LK8hWR/SC
aaRiMz8QdSUiIy0wDQYJKoZIhvcNAQELBQADggEBACh21WrMZLx+BGXohi5y1wlo
6kjEwkNBETOtt7DFpmmwg2AVDgfzDIhW35bFh80htWFhhzieBUe/vUNK6x3n4/+u
EHwd11II/iYoeKaYhBGyFNfgUQ40SdcdPqzH4I/ERd4UQXNl7XHYuSriXatjXxlF
USBfalMlnHROM3Im3TUpyDqmOjnSM4PRVNOlSNEuMUXwxbo+ueNxPKLNVVaA4n25
WDa62O0kJLqYAWTCi8hlZl3gIHYls+KNtmveZkbqJGjSwlbd/zoRVxfbt1L0oXtT
d7naClNAf66IdIZGAT27+B5tRow4tZ4Y/hzg2yo7Af1FeKLi0jtp+t9hgh77tV0=
-----END CERTIFICATE-----"#;

const SERVER_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC85iyEx46fVzhA
cGQg8QKxShbGFaQLycgfY78E4/49qvosj1p0H9kMuaCvs3asSq6S8T/dgnxAxltj
AY08DphAr+26dMW7f7FBH/VBej8oYCgyamjAvh1v2MK65tawS1phIpfGEW91qZhb
my9LqxNH7/2rkKvxp4it4VmFCm6GDcnfygUTrvDveaMMIOlEi93FM5KrJJRlXJwd
iKGYDZQH9BaQPVL0BiCE53PMDuDJsZH/Na+iSuLzpGa8iVFitfQ85UqIEP/Z2OHE
S/rODGC9w3dLYISby63lV3XloAUVDciv5KkWol0y+ZcIQNNNXhO/gBE892zSUjw6
EeI5yJPhAgMBAAECggEABl1vW11YCDUCi/HBG0wpA1f3ll7F+M1FD/MKafvwe1Sx
XeKtlNw6h1agymkLKFy2S4Lov9TYd/i6oM2Sv6xAqsI4JmPZAnnCgoeKT1Wr5ej/
GqJz0NJarpIsR866NDH4eSZsMgMwuFNNVshrWpylCvPigcu0w/ELRpLaQaEyohPH
joJ/C6YsYAcc3123mRz23CBvETYJzQYs9r7ci88iPrI85B7/5pZUSk5OoRlUi2sG
djagAFnBIdalKpxTcMuf2stzcC6CI2ouB0qY0odTZwiSVRP9+eo/EkB1CysXL2sl
9IATCqTC/ocKSc/1YK6g4JHns+z8tbTBIP8FeyRpGwKBgQD+BrCvN+jCB63teTAs
Mye3JMPBoPgChAnHR2hzGuMbsKBJdwtWLQSGCtMov81TRihWoHLgyWhuYN/UCXQJ
V9aSWF7Kd25Lze1mHnTmJHXhFMNFEtCu+h2QAVMvCmbY/ODBPs0VAXzqIW2My4aj
0G12tF0Bnf7KT/6mKLi3byI8HwKBgQC+Xe7L5p7FgrIOR76d8Rd8wbZpyHnhEhl7
HHz3juUTJlSncDfJplVI+Hyip6K/bIrI/Iwnz7w9bRkSGZOmNofzAtFgEQc2xSXd
kJQlKuTrL7EwF1SR6JOAB6k9T1qsRT7ytKr+oh+9fHd/Qd7NkIfq6tTRJ9wJWlyI
IJ7UTbsv/wKBgGBMEPabbzT+zERVyJk42zlmSn9AkkQB4eMVgtb/vlBk6J5w7m9A
qZJW0C2GaEPFOM1+DY6BS4FsX+11l/NixQi9T1HZbIp4CiLIMPB9qeIliNDKjSmH
z2Uj23DdtJdVZa5cLEpmQgBPo8PX87Zt8NErFobiahAvuw0qKrv++S9pAoGBAJDj
PVTDeiQpjQuBX3smfBHf/c4VX24GMI6a6CIjCAbDLbsildNMXazkMzg5Do1TN24x
iRrj6Ql3d5VnEhF3f5Fdm63aR/tPobo4yAhh1UmLSvinSR6kPV88dXrMYt6q9XYU
O/EBw9acXPbmU4Vxc4FAqilmhPo5ZCPXcAt1/fpRAoGAVNkKNIXqVu0jhxRFHSrp
Jx5X998LumoiRU38dmtkaKf+epfuYG4aZWVm7atA2Pwf8aaIBCXMdn5rnDDS4yFW
I9wR6akyXpNwmrFC5HdKh54WgXTZ/Qd8vgXQLhTXzizup9ksHOrLIYhsB+iXgCut
eFojDBvo55ePR948X9OHrfg=
-----END PRIVATE KEY-----"#;

const CLIENT_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDDDCCAfSgAwIBAgIUExeNy5Q72Lhe/lXf4wFtZYqxdQ4wDQYJKoZIhvcNAQEL
BQAwFjEUMBIGA1UEAwwLQUZTIFRlc3QgQ0EwHhcNMjYwOTI2MTMxNjA1WhcNMzYw
OTIzMTMxNjA1WjARMQ8wDQYDVQQDDAZub2RlLWIwggEiMA0GCSqGSIb3DQEBAQUA
A4IBDwAwggEKAoIBAQC//rRMEA/BAE181chtQetQA0TdLujKOPnYgITRznUpM16H
+c5H3eUtgaOEk0cVDSEFf77GDKzkPYDzICFeHtYZnzG3OjDlMaMCpaGTfYK3kWDE
VF0vwLgnPmbyf5giaoMEfRp+QP5/Tks3FlLZFeAD4KAhRfO2Wy2brIFUgc+K/V5D
YnXwgO9ptnywtpix1Er+0/zY0LsJzQE5QOunDPBh3vaaumVpyaoBvuDMZehVgCZF
td4Snb++zBwybWuF29yRPBQKUhO35YjY0NcQA11kKYLGQqt+w4PPRq43WJFA0NdQ
ITfBAHBVnGL8/nLf4JrfO5MRVrdE5J6nyMOUv2mXAgMBAAGjVzBVMBMGA1UdJQQM
MAoGCCsGAQUFBwMCMB0GA1UdDgQWBBS7kzepqT9ulwNaZdEkwDI1vl29sjAfBgNV
HSMEGDAWgBQRrosryFZH9IJppGIzPxB1JSIjLTANBgkqhkiG9w0BAQsFAAOCAQEA
BC6Zn7q/ctyOrylb92ejJBo/A1NmOt2OIR071kO0MV14bHIvCTVJoDEgZCT4XAQt
hKMUILtJjektvUvQb8Ah95qy7jXfd/AdxcDaQI3fYxoEvd7t6TxY09A6tL45Vbwq
pJRRMyo+kljkGoFsO4EZhGCV9iaw+ND1n1GiwS0fU9JihlwZ0rc+uLPd0tnp6Juj
fJlIAjrkaKVKOuNohUolCqIENn6ojD26l0eUrJ5kquJ+CXLLsH3kMuIDjAHU3E6j
bnJDCbTL/pQI2mqaZIxsDPJV6nAAW0IvH9AdI2B5w73LKxqWvKMFNy+eawlWyEst
RmIJ/4aMkpeOjj4nJcVloQ==
-----END CERTIFICATE-----"#;

const CLIENT_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC//rRMEA/BAE18
1chtQetQA0TdLujKOPnYgITRznUpM16H+c5H3eUtgaOEk0cVDSEFf77GDKzkPYDz
ICFeHtYZnzG3OjDlMaMCpaGTfYK3kWDEVF0vwLgnPmbyf5giaoMEfRp+QP5/Tks3
FlLZFeAD4KAhRfO2Wy2brIFUgc+K/V5DYnXwgO9ptnywtpix1Er+0/zY0LsJzQE5
QOunDPBh3vaaumVpyaoBvuDMZehVgCZFtd4Snb++zBwybWuF29yRPBQKUhO35YjY
0NcQA11kKYLGQqt+w4PPRq43WJFA0NdQITfBAHBVnGL8/nLf4JrfO5MRVrdE5J6n
yMOUv2mXAgMBAAECggEAGJwc3z0Vz97qj8xVXw/aikyI+LMJGta3y9UZcU09/lR7
0wElvFeIh089NwKr01p19576xKceSDlL/J4LOOXJ+snJlRtr5gz5QJ8beWzWoxIK
7c+EjFjvIfShPIc3aH3volUo3rMVDBtsj7iYUQQ5TTXvQKSXSzIfw/sWLs9e24lK
hvCFrXT8rsSollJ1L2UKaW/2VtUvqt1HJV+oCbxN7snDy81Dc9YtOHWepGAcfe4r
tUJzsteBnUbTDEWXAaRYmiW+bA69PPlVUd9AwROaZxbcAenl3g75MVRZihc7TbOc
DVT1ycwuPq6z+fcyMWIkh4P32oUqTkNMtwFkJxeuPQKBgQDzDcSU1Cbt6dHINbWB
mCCkYPBFA/u0GikEd4+ChoMiHILSx89xk//1Wgp2xubm3W+z8/EuKas3S51EZhO1
Mi9FO4vAeDsNu+kpEG+UUf7Ux3aRC6fS0DH53+X57coKE3m+cQV/jG8BPB7xZnra
Qi6O8jW/WNJ6fqYwwAgHA5uhIwKBgQDKOLVXTmaWZ6P/P/1AD/0hZhB2FtLZIa5g
94wY88qenxXwjHl1SYaRosm6Vjrx/JuK6hMJvU1FlJ2AMlv1dnMvatPTtMubA0Rc
ZNFjKC321QH2G+HIPggPrYlcbnfbuvURjzgmAwxV+kVGS0CX6aSExGaGtl4WurTD
EmYGWI3O/QKBgQDiZAybZBDuwkA48G4kTAL7mZ+zaUZmN4fFNWhi98/lUhE5LAw5
itV7P2dHw3UHzXJid/JKQV3Nn4zZTQtGV3xYTGKb4GGBJWrEaR7FVKq8nx39dJHZ
dztVuAuKhMcQI5vem3+3kqNCzzEzQXVlHwgm9czCcoV6u8Uo23WesumfaQKBgEMX
I1rO4Qw/YFJ7+Vp6s4GUKhvzoIp3OTJkjq9smqmboBzJjjZSaXoB5ymSGEZWh4hD
9oMBshRGpSZ2DrpWTQrLR3HyhqZsJA7/R9S87Nr6eocbYwIbSnNhILRw1gUpdssX
mApMcphHyxnyN4Du/C0sN9Ozx22FDhm2DfFHCe1FAoGBAJBAkyS0KGaYgs/8rAbL
cVSIL0zATu8Kv73EX7dHBzf5nZxAlQQ2nzNzjxv3Z8AlAz5/dztq2PSmcy5Tarpt
w1TlUs4iPMwsEQNBl8pRelqXHu/aUJ++fJ1Ax2HnziEWJO6q24YyyBafhHYyte52
zNEGIq4pivkTMT3xJGFmgGqs
-----END PRIVATE KEY-----"#;

#[derive(Default)]
struct ContractMeta {
    active: Mutex<Option<RootGrant>>,
    validate_calls: Mutex<usize>,
    block_on_validation: bool,
}

impl ContractMeta {
    #[cfg(feature = "rdma")]
    fn with_block_on_validation() -> Self {
        Self {
            block_on_validation: true,
            ..Self::default()
        }
    }

    fn grant_for(&self, holder: &str, session: &str, right: RootRight) -> RootGrant {
        let active = self
            .active
            .lock()
            .unwrap()
            .clone()
            .expect("active root grant");
        RootGrant {
            id: active.id,
            epoch: active.epoch,
            home_node_id: active.home_node_id,
            home_session_id: active.home_session_id,
            holder_node_id: holder.to_owned(),
            session_id: session.to_owned(),
            access_generation: active.access_generation,
            rights: vec![RootRight::Lookup, RootRight::Read, RootRight::Write, right],
            fencing_token: active.fencing_token,
        }
    }
}

impl RootMeta for ContractMeta {
    fn reserve_root(
        &self,
        id: &RootId,
        create_intent_id: &str,
    ) -> afs_error::Result<RootReservation> {
        Ok(RootReservation {
            id: id.clone(),
            epoch: 1,
            home_node_id: "node-a".to_owned(),
            session_id: "session-a".to_owned(),
            create_intent_id: create_intent_id.to_owned(),
            prepare_token: "prepare-token".to_owned(),
        })
    }

    fn activate_root(&self, prepared: &PreparedRoot) -> afs_error::Result<RootGrant> {
        let grant = RootGrant {
            id: prepared.reservation().id().clone(),
            epoch: prepared.reservation().epoch,
            home_node_id: "node-a".to_owned(),
            home_session_id: "session-a".to_owned(),
            holder_node_id: "node-a".to_owned(),
            session_id: "session-a".to_owned(),
            access_generation: 1,
            rights: vec![RootRight::Lookup, RootRight::Read, RootRight::Write],
            fencing_token: "fence-1".to_owned(),
        };
        *self.active.lock().unwrap() = Some(grant.clone());
        Ok(grant)
    }

    fn abort_root(&self, _reservation: &RootReservation) -> afs_error::Result<()> {
        Ok(())
    }

    fn lookup_root(&self, id: &RootId) -> afs_error::Result<Option<RootLocation>> {
        Ok(self
            .active
            .lock()
            .unwrap()
            .as_ref()
            .filter(|grant| &grant.id == id)
            .map(|grant| RootLocation {
                id: grant.id.clone(),
                epoch: grant.epoch,
                home_node_id: grant.home_node_id.clone(),
                home_session_id: grant.home_session_id.clone(),
            }))
    }

    fn list_owner_roots(
        &self,
        home_node_id: &str,
    ) -> afs_error::Result<afs::node::vfs::ownerfs::root::OwnerRootInventory> {
        Ok(afs::node::vfs::ownerfs::root::OwnerRootInventory {
            active: self
                .active
                .lock()
                .unwrap()
                .as_ref()
                .filter(|grant| grant.home_node_id == home_node_id)
                .map(|grant| RootLocation {
                    id: grant.id.clone(),
                    epoch: grant.epoch,
                    home_node_id: grant.home_node_id.clone(),
                    home_session_id: grant.home_session_id.clone(),
                })
                .into_iter()
                .collect(),
            pending: Vec::new(),
        })
    }

    fn acquire_root(&self, id: &RootId, right: RootRight) -> afs_error::Result<RootGrant> {
        let grant = self.grant_for("node-b", "session-b", right);
        assert_eq!(&grant.id, id);
        Ok(grant)
    }

    fn validate_root_access(
        &self,
        presented: &afs::node::vfs::ownerfs::root::PresentedRootAccess,
        authenticated_peer_node_id: &str,
    ) -> afs_error::Result<RootGrant> {
        assert_eq!(authenticated_peer_node_id, "node-b");
        assert_eq!(presented.holder_node_id, "node-b");
        if self.block_on_validation {
            tokio::runtime::Handle::current().block_on(async {});
        }
        *self.validate_calls.lock().unwrap() += 1;
        Ok(self.grant_for(
            &presented.holder_node_id,
            &presented.session_id,
            RootRight::Write,
        ))
    }

    fn recover_root(
        &self,
        _record: &LocalRootRecord,
        _new_session_id: &str,
    ) -> afs_error::Result<RootGrant> {
        Err(afs_error::Error::coded(
            afs_error::META_STORE_UNIMPLEMENTED,
            "recovery is outside this peer contract test",
        ))
    }
}

#[derive(Clone)]
struct RequireMtlsNodeB;

impl PeerAuthenticator for RequireMtlsNodeB {
    fn authenticate(
        &self,
        _metadata: &MetadataMap,
        _remote_addr: Option<std::net::SocketAddr>,
        peer_cert_der: Option<&[u8]>,
    ) -> afs_error::Result<String> {
        if peer_cert_der.is_none() {
            return Err(afs_error::Error::coded(
                afs_error::NODE_OWNER_INVALID_GRANT,
                "test OwnerFiles peer must present mTLS certificate",
            ));
        }
        Ok("node-b".to_owned())
    }
}

#[cfg(feature = "rdma")]
struct OwnerRdmaFixture {
    _temp: tempfile::TempDir,
    meta: Arc<ContractMeta>,
    ctx: RequestContext,
    grant: RootGrant,
    channel: Channel,
    rdma_registry: RdmaSessionRegistry,
    metrics_registry: afs_metrics::Registry,
    metrics: afs::node::rpc::OwnerRpcMetrics,
    server: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "rdma")]
fn rdma_device() -> Option<String> {
    std::env::var("AFS_TEST_RDMA_DEVICE")
        .ok()
        .filter(|value| !value.is_empty())
}

#[cfg(feature = "rdma")]
fn owner_payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| ((index.wrapping_mul(37).wrapping_add(index / 251)) & 0xff) as u8)
        .collect()
}

#[cfg(feature = "rdma")]
fn owner_payload_bytes(
    registry: &afs_metrics::Registry,
    side: &str,
    direction: &str,
    plane: &str,
) -> u64 {
    afs_metrics::encode_text(registry)
        .expect("encode metrics")
        .lines()
        .find_map(|line| {
            if line.starts_with("afs_ownerfiles_payload_bytes_total{")
                && line.contains(&format!("side=\"{side}\""))
                && line.contains(&format!("direction=\"{direction}\""))
                && line.contains(&format!("plane=\"{plane}\""))
            {
                line.rsplit_once(' ')
                    .and_then(|(_, value)| value.parse::<u64>().ok())
            } else {
                None
            }
        })
        .unwrap_or(0)
}

#[cfg(feature = "rdma")]
async fn wait_for_rdma_checkpoint(path: &std::path::Path) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("GDB must prove data WQE posted before the client deadline");
}

#[cfg(feature = "rdma")]
async fn wait_for_endpoint_release(
    endpoint: std::sync::Weak<afs::node::rpc::control::RdmaServerEndpoint>,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while endpoint.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("last server endpoint owner must release after resume");
}

#[cfg(feature = "rdma")]
async fn wait_for_owner_rdma_lookup_stale(
    registry: &RdmaSessionRegistry,
    session_id: u64,
    identity: &PeerSessionIdentity,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match registry.session_for(session_id, identity).await {
                Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(status) => {
                    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                    if status.message() == "RDMA session poisoned" {
                        // The RPC can poison the session before the separate
                        // close guard removes its registry lookup.
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    assert!(
                        status.message().contains("unknown RDMA session"),
                        "stale lookup must prove removal, not a live poisoned session: {status:?}"
                    );
                    break;
                }
            }
        }
    })
    .await
    .expect("OwnerCloseData guard must remove stale RDMA lookup");
}

#[cfg(feature = "rdma")]
fn save_rdma_resources(directory: &std::path::Path, phase: &str) {
    let tids: Vec<u32> = std::fs::read_dir("/proc/self/task")
        .expect("Linux thread inventory")
        .map(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .parse()
                .unwrap()
        })
        .collect();
    std::fs::write(
        directory.join(format!("{phase}-process.json")),
        serde_json::to_vec(&serde_json::json!({"pid": std::process::id(), "tids": tids})).unwrap(),
    )
    .expect("save process identity");
    for kind in ["qp", "mr", "cq", "pd", "ctx"] {
        let output = std::process::Command::new("rdma")
            .args(["-j", "resource", "show", kind])
            .output()
            .expect("rdma resource inventory");
        assert!(output.status.success(), "RDMA {kind} inventory failed");
        std::fs::write(
            directory.join(format!("{phase}-{kind}.json")),
            output.stdout,
        )
        .expect("save exact RDMA resources");
    }
}

#[cfg(feature = "rdma")]
fn owner_presented_access_for(
    grant: &RootGrant,
) -> afs::node::vfs::ownerfs::root::PresentedRootAccess {
    afs::node::vfs::ownerfs::root::PresentedRootAccess {
        id: grant.id.clone(),
        epoch: grant.epoch,
        home_node_id: grant.home_node_id.clone(),
        home_session_id: grant.home_session_id.clone(),
        holder_node_id: grant.holder_node_id.clone(),
        session_id: grant.session_id.clone(),
        access_generation: grant.access_generation,
        fencing_token: grant.fencing_token.clone(),
    }
}

#[cfg(feature = "rdma")]
fn owner_fixture_file_path(fixture: &OwnerRdmaFixture, file_name: &str) -> std::path::PathBuf {
    fixture
        ._temp
        .path()
        .join(format!("{}-e{}", fixture.grant.id.0, fixture.grant.epoch))
        .join(file_name)
}

#[cfg(feature = "rdma")]
fn assert_owner_deadline(error: &afs_error::Error) {
    assert!(
        error.kind() == afs_error::ErrorKind::DeadlineExceeded
            || (error.code() == afs_error::CLIENT_REMOTE_STATUS
                && error.kind() == afs_error::ErrorKind::Cancelled
                && error.message() == "Timeout expired"),
        "posted OwnerPeerClient write must fail with the request deadline, got {error:?}"
    );
}

#[cfg(feature = "rdma")]
async fn owner_rdma_fixture(device: Option<String>, root_name: &str) -> OwnerRdmaFixture {
    owner_rdma_fixture_with_authority(device, root_name, ContractMeta::default()).await
}

#[cfg(feature = "rdma")]
async fn owner_rdma_fixture_with_authority(
    device: Option<String>,
    root_name: &str,
    authority: ContractMeta,
) -> OwnerRdmaFixture {
    let temp = tempfile::tempdir().expect("tempdir");
    let disk = Arc::new(LocalFs::open(temp.path()).expect("localfs"));
    let meta = Arc::new(authority);
    let roots = Arc::new(RootManager::new(
        "node-a".to_owned(),
        "session-a".to_owned(),
        meta.clone(),
        disk.clone(),
    ));
    let fs = Arc::new(OwnerFs::new_local(roots, disk));
    let ctx = RequestContext {
        uid: temp.path().metadata().expect("fixture metadata").uid(),
        gid: temp.path().metadata().expect("fixture metadata").gid(),
        pid: 42,
        umask: 0,
        supplementary_gids: Vec::new(),
    };
    let owner_root = BackendInode { value: 1 };
    fs.mkdir(&ctx, owner_root, OsStr::new(root_name), 0o755)
        .expect("create home root");
    let root_id = root_id_from_name(OsStr::new(root_name)).expect("root id");
    let grant = meta
        .acquire_root(&root_id, RootRight::Write)
        .expect("remote grant");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let executor = fs.peer_executor().expect("peer executor");
    let authenticator = Arc::new(RequireMtlsNodeB);
    let rdma_registry = RdmaSessionRegistry::new(device);
    let metrics_registry = afs_metrics::Registry::new();
    let metrics =
        afs::node::rpc::OwnerRpcMetrics::register(&metrics_registry).expect("Owner metrics");
    let control = afs::node::rpc::control::NodeControlService::new(RdmaSessionRegistry::new(None))
        .with_owner_locks(executor.clone(), authenticator.clone())
        .with_owner_rdma_registry(rdma_registry.clone())
        .into_server();
    let handler = make_owner_files_handler(executor);
    let owner_files = OwnerFilesServer::new(
        OwnerFilesService::new(handler, authenticator)
            .with_metrics(metrics.clone())
            .with_rdma_registry(rdma_registry.clone()),
    );
    let server_tls = ServerTlsConfig::new()
        .client_ca_root(Certificate::from_pem(CA_PEM))
        .identity(Identity::from_pem(SERVER_CERT_PEM, SERVER_KEY_PEM));
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls)
            .expect("server tls")
            .add_service(control)
            .add_service(owner_files)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("owner RDMA files server");
    });

    let client_tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(CA_PEM))
        .identity(Identity::from_pem(CLIENT_CERT_PEM, CLIENT_KEY_PEM))
        .domain_name("localhost");
    let channel: Channel = Endpoint::from_shared(endpoint)
        .expect("endpoint")
        .timeout(Duration::from_secs(10))
        .tls_config(client_tls)
        .expect("client tls")
        .connect()
        .await
        .expect("connect");

    OwnerRdmaFixture {
        _temp: temp,
        meta,
        ctx,
        grant,
        channel,
        rdma_registry,
        metrics_registry,
        metrics,
        server,
    }
}

#[cfg(feature = "rdma")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ownerfiles_mtls_statfs_roundtrip_reports_home_capacity() {
    let fixture = owner_rdma_fixture(None, "statfs-home").await;
    let expected = LocalFs::open(fixture._temp.path())
        .expect("localfs")
        .statvfs()
        .expect("fixture capacity");
    let client = owner_files_client_from_channel_with_runtime(
        fixture.channel.clone(),
        tokio::runtime::Handle::current(),
    );

    let grant = fixture.grant.clone();
    let capacity = tokio::task::spawn_blocking(move || {
        client
            .statfs(&grant, OsStr::new(""), None)
            .expect("statfs over OwnerFiles")
    })
    .await
    .expect("statfs worker");

    assert_eq!(capacity.blocks, expected.blocks);
    assert_eq!(capacity.files, expected.files);
    assert_eq!(capacity.bsize, expected.bsize);
    assert_eq!(capacity.frsize, expected.frsize);
    assert_eq!(capacity.namelen, expected.namelen);
    assert!(capacity.bfree <= capacity.blocks);
    assert!(capacity.bavail <= capacity.bfree);
    assert!(capacity.ffree <= capacity.files);
    assert_eq!(*fixture.meta.validate_calls.lock().unwrap(), 1);

    fixture.server.abort();
}

#[cfg(feature = "rdma")]
#[derive(Default)]
struct PrefetchDespiteDisableHandler {
    open_disable_prefetch: Mutex<Vec<bool>>,
    read_calls: Mutex<usize>,
}

#[cfg(feature = "rdma")]
impl OwnerFilesHandler for PrefetchDespiteDisableHandler {
    fn open(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerOpenRequest,
    ) -> afs_error::Result<OwnerOpenReply> {
        assert_eq!(authenticated_peer_node_id, "node-b");
        self.open_disable_prefetch
            .lock()
            .unwrap()
            .push(request.disable_prefetch);
        let identity = FileIdentity {
            opaque: b"prefetch-identity".to_vec(),
        };
        Ok(OwnerOpenReply {
            handle: Some(OwnerHandle {
                opaque: b"prefetch-handle".to_vec(),
            }),
            file_identity: Some(identity.clone()),
            owner_session_id: "owner-session-prefetch".to_owned(),
            attr: Some(OwnerFileAttr {
                identity: Some(identity),
                kind: OwnerFileKind::Regular.into(),
                mode: 0o644,
                uid: 1,
                gid: 1,
                size: 6,
                blocks: 1,
                atime_ns: 0,
                mtime_ns: 0,
                ctime_ns: 0,
                nlink: 1,
                blksize: 4096,
                special_node: None,
            }),
            prefetched_data: Some(b"cached".to_vec()),
        })
    }

    fn read(
        &self,
        authenticated_peer_node_id: &str,
        _request: OwnerReadRequest,
    ) -> afs_error::Result<OwnerReadReply> {
        assert_eq!(authenticated_peer_node_id, "node-b");
        *self.read_calls.lock().unwrap() += 1;
        Ok(OwnerReadReply {
            data: b"cached".to_vec().into(),
            read: 6,
            eof: true,
            data_checksum: blake3::hash(b"cached").as_bytes().to_vec(),
        })
    }
}

#[cfg(feature = "rdma")]
#[derive(Default)]
struct MalformedStatFsHandler;

#[cfg(feature = "rdma")]
impl OwnerFilesHandler for MalformedStatFsHandler {
    fn statfs(
        &self,
        authenticated_peer_node_id: &str,
        _request: OwnerStatFsRequest,
    ) -> afs_error::Result<OwnerStatFsReply> {
        assert_eq!(authenticated_peer_node_id, "node-b");
        Ok(OwnerStatFsReply {
            capacity: Some(OwnerFilesystemCapacity {
                blocks: 1,
                bfree: 2,
                bavail: 0,
                files: 1,
                ffree: 0,
                bsize: 0,
                namelen: 255,
                frsize: 4096,
            }),
        })
    }
}

#[cfg(feature = "rdma")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ownerpeerclient_rejects_malformed_statfs_capacity_shape() {
    let (channel, server) = prefetch_despite_disable_server(Arc::new(MalformedStatFsHandler)).await;
    let grant = RootGrant {
        id: RootId("statfs-shape".to_owned()),
        epoch: 1,
        home_node_id: "node-a".to_owned(),
        home_session_id: "session-a".to_owned(),
        holder_node_id: "node-b".to_owned(),
        session_id: "session-b".to_owned(),
        access_generation: 1,
        rights: vec![RootRight::Read],
        fencing_token: "fence".to_owned(),
    };
    let client =
        owner_files_client_from_channel_with_runtime(channel, tokio::runtime::Handle::current());

    let error = tokio::task::spawn_blocking(move || {
        client.statfs(&grant, OsStr::new(""), None).unwrap_err()
    })
    .await
    .expect("statfs worker");

    assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
    assert!(error.message().contains("capacity shape"));
    server.abort();
}

#[cfg(feature = "rdma")]
async fn prefetch_despite_disable_server(
    handler: Arc<dyn OwnerFilesHandler>,
) -> (Channel, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let server_tls = ServerTlsConfig::new()
        .client_ca_root(Certificate::from_pem(CA_PEM))
        .identity(Identity::from_pem(SERVER_CERT_PEM, SERVER_KEY_PEM));
    let owner_files =
        OwnerFilesServer::new(OwnerFilesService::new(handler, Arc::new(RequireMtlsNodeB)));
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls)
            .expect("server tls")
            .add_service(owner_files)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("prefetch OwnerFiles server");
    });

    let client_tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(CA_PEM))
        .identity(Identity::from_pem(CLIENT_CERT_PEM, CLIENT_KEY_PEM))
        .domain_name("localhost");
    let channel = Endpoint::from_shared(endpoint)
        .expect("endpoint")
        .timeout(Duration::from_secs(10))
        .tls_config(client_tls)
        .expect("client tls")
        .connect()
        .await
        .expect("connect");
    (channel, server)
}

#[cfg(feature = "rdma")]
fn prefetch_test_grant() -> RootGrant {
    RootGrant {
        id: RootId("job-prefetch-trust".to_owned()),
        epoch: 1,
        home_node_id: "node-a".to_owned(),
        home_session_id: "home-session".to_owned(),
        holder_node_id: "node-b".to_owned(),
        session_id: "grant-session".to_owned(),
        access_generation: 1,
        rights: vec![RootRight::Lookup, RootRight::Read],
        fencing_token: "fence".to_owned(),
    }
}

#[cfg(feature = "rdma")]
async fn negotiate_owner_rdma_window(
    channel: Channel,
    access: RootAccess,
    device: String,
) -> (RdmaEndpoint, u64) {
    let mut endpoint = tokio::task::spawn_blocking(move || RdmaEndpoint::open(&device))
        .await
        .expect("open RDMA endpoint worker")
        .expect("open RDMA endpoint");
    let client_info = endpoint.info().expect("client RDMA info").to_vec();
    let capacity = endpoint.capacity();
    let mut control = NodeControlClient::new(channel);
    let reply = control
        .owner_negotiate_data(tonic::Request::new(OwnerNegotiateDataRequest {
            access: Some(access),
            negotiation: Some(NegotiateDataRequest {
                client_info,
                capacity: capacity as u32,
                handshake_version: RDMA_HANDSHAKE_VERSION,
            }),
        }))
        .await
        .expect("Owner RDMA negotiate")
        .into_inner();
    assert!(
        reply.rdma_supported,
        "Owner RDMA negotiation did not enable RDMA"
    );
    assert_ne!(reply.session_id, 0);
    assert_eq!(reply.handshake_version, RDMA_HANDSHAKE_VERSION);
    assert_eq!(reply.capacity as usize, capacity);
    endpoint = tokio::task::spawn_blocking(move || {
        endpoint.connect(&reply.server_info)?;
        endpoint.send_probe(5000)?;
        Ok::<_, afs_transport::rdma::RdmaError>(endpoint)
    })
    .await
    .expect("connect RDMA endpoint worker")
    .expect("connect RDMA endpoint");
    (endpoint, reply.session_id)
}

#[cfg(feature = "rdma")]
async fn close_owner_rdma_window(channel: Channel, access: RootAccess, session_id: u64) {
    NodeControlClient::new(channel)
        .owner_close_data(tonic::Request::new(OwnerCloseDataRequest {
            access: Some(access),
            session_id,
        }))
        .await
        .expect("close Owner RDMA window");
}

#[cfg(feature = "rdma")]
async fn owner_rdma_write_window(
    raw: &mut OwnerFilesClient<Channel>,
    control_channel: Channel,
    access: RootAccess,
    handle: afs_protocol::node_data::OwnerHandle,
    offset: u64,
    data: &[u8],
    device: String,
) {
    let (mut endpoint, session_id) =
        negotiate_owner_rdma_window(control_channel.clone(), access.clone(), device).await;
    let payload = data.to_vec();
    let _endpoint = tokio::task::spawn_blocking({
        let payload = payload.clone();
        move || {
            endpoint.put_local(&payload)?;
            Ok::<_, afs_transport::rdma::RdmaError>(endpoint)
        }
    })
    .await
    .expect("stage Owner RDMA write worker")
    .expect("stage Owner RDMA write payload");
    let reply = raw
        .write(OwnerWriteRequest {
            access: Some(access.clone()),
            handle: Some(handle),
            offset,
            data: Vec::new(),
            length: payload.len() as u32,
            plane: Some(DataPlane {
                transfer: DataTransfer::RdmaOneSided.into(),
                rdma_session_id: session_id,
                buffer_offset: 0,
            }),
            kill_suidgid: false,
            data_checksum: blake3::hash(&payload).as_bytes().to_vec(),
        })
        .await
        .expect("Owner RDMA write RPC")
        .into_inner();
    assert_eq!(reply.written as usize, payload.len());
    close_owner_rdma_window(control_channel, access, session_id).await;
}

#[cfg(feature = "rdma")]
async fn owner_rdma_read_window(
    raw: &mut OwnerFilesClient<Channel>,
    control_channel: Channel,
    access: RootAccess,
    handle: afs_protocol::node_data::OwnerHandle,
    offset: u64,
    length: usize,
    device: String,
) -> (Vec<u8>, bool) {
    let (mut endpoint, session_id) =
        negotiate_owner_rdma_window(control_channel.clone(), access.clone(), device).await;
    let reply = raw
        .read(OwnerReadRequest {
            access: Some(access.clone()),
            handle: Some(handle),
            offset,
            length: length as u32,
            plane: Some(DataPlane {
                transfer: DataTransfer::RdmaOneSided.into(),
                rdma_session_id: session_id,
                buffer_offset: 0,
            }),
        })
        .await
        .expect("Owner RDMA read RPC")
        .into_inner();
    assert!(
        reply.data.is_empty(),
        "RDMA read must not return inline data"
    );
    assert!(reply.read as usize <= length);
    let read_len = reply.read as usize;
    let bytes = tokio::task::spawn_blocking(move || endpoint.get_local(read_len))
        .await
        .expect("collect Owner RDMA read worker")
        .expect("collect Owner RDMA read payload");
    assert_eq!(
        blake3::hash(&bytes).as_bytes().as_slice(),
        reply.data_checksum.as_slice()
    );
    close_owner_rdma_window(control_channel, access, session_id).await;
    (bytes, reply.eof)
}

#[cfg(feature = "rdma")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working RXE/RDMA device"]
async fn ownerfiles_rdma_large_write_fsync_cold_read_roundtrip_preserves_payload() {
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let fixture = owner_rdma_fixture(Some(device.clone()), "job-rdma").await;
    let access = root_access_for(&fixture.grant);
    let mut raw = OwnerFilesClient::new(fixture.channel.clone());
    let root = raw
        .lookup(OwnerLookupRequest {
            access: Some(access.clone()),
            path: Vec::new(),
            expected_parent_identity: None,
        })
        .await
        .expect("lookup root")
        .into_inner()
        .attr
        .expect("root attr")
        .identity
        .expect("root identity");
    let created = raw
        .create(OwnerCreateRequest {
            access: Some(access.clone()),
            path: b"rdma-large.bin".to_vec(),
            flags: libc::O_RDWR as u32,
            mode: 0o644,
            expected_parent: Some(root),
            caller: Some(OwnerCaller {
                uid: fixture.ctx.uid,
                gid: fixture.ctx.gid,
                pid: fixture.ctx.pid,
                umask: fixture.ctx.umask,
                supplementary_gids: fixture.ctx.supplementary_gids.clone(),
            }),
            kill_suidgid: false,
        })
        .await
        .expect("create RDMA file")
        .into_inner();
    let payload = owner_payload(4 * 1024 * 1024 + 17);
    let expected_hash = blake3::hash(&payload);
    let write_window = 1024 * 1024;
    let expected_windows = payload.chunks(write_window).count();
    let mut rdma_write_windows = 0usize;
    for (index, chunk) in payload.chunks(write_window).enumerate() {
        owner_rdma_write_window(
            &mut raw,
            fixture.channel.clone(),
            access.clone(),
            created.handle.clone().expect("created handle"),
            (index * write_window) as u64,
            chunk,
            device.clone(),
        )
        .await;
        rdma_write_windows += 1;
    }
    assert_eq!(rdma_write_windows, expected_windows);
    raw.fsync(OwnerFsyncRequest {
        access: Some(access.clone()),
        handle: created.handle.clone(),
        datasync: false,
    })
    .await
    .expect("fsync RDMA file");
    raw.release(OwnerReleaseRequest {
        access: Some(access.clone()),
        handle: created.handle,
    })
    .await
    .expect("release written RDMA file");

    let opened = raw
        .open(OwnerOpenRequest {
            access: Some(access.clone()),
            path: b"rdma-large.bin".to_vec(),
            flags: libc::O_RDONLY as u32,
            mode: 0,
            expected_file_identity: created.attr.and_then(|attr| attr.identity),
            kill_suidgid: false,
            disable_prefetch: true,
        })
        .await
        .expect("cold reopen RDMA file")
        .into_inner();
    assert!(opened.prefetched_data.is_none());
    let read_handle = opened.handle.clone().expect("read handle");
    let mut read_back = Vec::with_capacity(payload.len());
    let mut rdma_read_windows = 0usize;
    while read_back.len() < payload.len() {
        let remaining = payload.len() - read_back.len();
        let (bytes, eof) = owner_rdma_read_window(
            &mut raw,
            fixture.channel.clone(),
            access.clone(),
            read_handle.clone(),
            read_back.len() as u64,
            remaining.min(write_window),
            device.clone(),
        )
        .await;
        assert!(
            !bytes.is_empty(),
            "Owner RDMA read hit EOF before full payload"
        );
        rdma_read_windows += 1;
        read_back.extend_from_slice(&bytes);
        assert!(
            !eof,
            "full Owner RDMA read should not claim EOF; explicit zero read below proves EOF"
        );
    }
    let eof = raw
        .read(OwnerReadRequest {
            access: Some(access.clone()),
            handle: Some(read_handle.clone()),
            offset: payload.len() as u64,
            length: 1,
            plane: Some(DataPlane {
                transfer: DataTransfer::GrpcInline.into(),
                rdma_session_id: 0,
                buffer_offset: 0,
            }),
        })
        .await
        .expect("EOF read after Owner RDMA payload")
        .into_inner();
    assert_eq!(eof.read, 0);
    assert!(eof.data.is_empty());
    assert!(eof.eof);
    assert_eq!(rdma_read_windows, expected_windows);
    assert_eq!(blake3::hash(&read_back), expected_hash);
    assert_eq!(read_back, payload);
    raw.release(OwnerReleaseRequest {
        access: Some(access),
        handle: Some(read_handle),
    })
    .await
    .expect("release read handle");
    assert!(*fixture.meta.validate_calls.lock().unwrap() >= 1);
    fixture.server.abort();
}

#[cfg(feature = "rdma")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ownerfiles_rdma_missing_device_fails_before_file_write() {
    let fixture = owner_rdma_fixture(None, "job-rdma-missing").await;
    let access = root_access_for(&fixture.grant);
    let mut raw = OwnerFilesClient::new(fixture.channel.clone());
    let root = raw
        .lookup(OwnerLookupRequest {
            access: Some(access.clone()),
            path: Vec::new(),
            expected_parent_identity: None,
        })
        .await
        .expect("lookup root")
        .into_inner()
        .attr
        .expect("root attr")
        .identity
        .expect("root identity");
    let created = raw
        .create(OwnerCreateRequest {
            access: Some(access.clone()),
            path: b"missing-rdma-device.bin".to_vec(),
            flags: libc::O_RDWR as u32,
            mode: 0o644,
            expected_parent: Some(root),
            caller: Some(OwnerCaller {
                uid: fixture.ctx.uid,
                gid: fixture.ctx.gid,
                pid: fixture.ctx.pid,
                umask: fixture.ctx.umask,
                supplementary_gids: fixture.ctx.supplementary_gids.clone(),
            }),
            kill_suidgid: false,
        })
        .await
        .expect("create missing-device target")
        .into_inner();
    let missing = "__afs_missing_owner_rdma_device__".to_owned();
    let open = tokio::task::spawn_blocking(move || RdmaEndpoint::open(&missing))
        .await
        .expect("missing-device open worker");
    assert!(
        open.is_err(),
        "test fixture unexpectedly opened fake RDMA device"
    );
    let attr = raw
        .get_attr(OwnerGetAttrRequest {
            access: Some(access.clone()),
            path: b"missing-rdma-device.bin".to_vec(),
            expected_file_identity: created.attr.and_then(|attr| attr.identity),
            handle: created.handle.clone(),
        })
        .await
        .expect("getattr after missing RDMA device")
        .into_inner()
        .attr
        .expect("attr after missing RDMA device");
    assert_eq!(attr.size, 0);
    let read = raw
        .read(OwnerReadRequest {
            access: Some(access.clone()),
            handle: created.handle.clone(),
            offset: 0,
            length: 1,
            plane: Some(DataPlane {
                transfer: DataTransfer::GrpcInline.into(),
                rdma_session_id: 0,
                buffer_offset: 0,
            }),
        })
        .await
        .expect("read after missing RDMA device")
        .into_inner();
    assert_eq!(read.read, 0);
    assert!(read.data.is_empty());
    raw.release(OwnerReleaseRequest {
        access: Some(access),
        handle: created.handle,
    })
    .await
    .expect("release missing-device handle");
    assert!(*fixture.meta.validate_calls.lock().unwrap() >= 1);
    fixture.server.abort();
}

#[cfg(feature = "rdma")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ownerfiles_rdma_authority_validation_may_block_on_tokio_handle() {
    let fixture = owner_rdma_fixture_with_authority(
        None,
        "job-rdma-authority-block-on",
        ContractMeta::with_block_on_validation(),
    )
    .await;
    let mut access = root_access_for(&fixture.grant);
    access.session_id = "session-fresh-negotiate".to_owned();
    let before = *fixture.meta.validate_calls.lock().unwrap();
    let reply = NodeControlClient::new(fixture.channel.clone())
        .owner_negotiate_data(tonic::Request::new(OwnerNegotiateDataRequest {
            access: Some(access),
            negotiation: Some(NegotiateDataRequest {
                client_info: Vec::new(),
                capacity: 0,
                handshake_version: RDMA_HANDSHAKE_VERSION,
            }),
        }))
        .await
        .expect("authority validation should run without Tokio block_on panic")
        .into_inner();
    assert!(!reply.rdma_supported);
    assert_eq!(reply.session_id, 0);
    assert_eq!(reply.capacity, 0);
    assert_eq!(reply.handshake_version, RDMA_HANDSHAKE_VERSION);
    assert_eq!(*fixture.meta.validate_calls.lock().unwrap(), before + 1);
    fixture.server.abort();
}

#[cfg(feature = "rdma")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ownerfiles_rdma_oversize_read_rejects_before_handler_allocation() {
    let fixture = owner_rdma_fixture(
        Some("__unused_oversize_device__".to_owned()),
        "job-rdma-oversize",
    )
    .await;
    let access = root_access_for(&fixture.grant);
    let mut raw = OwnerFilesClient::new(fixture.channel.clone());
    let root = raw
        .lookup(OwnerLookupRequest {
            access: Some(access.clone()),
            path: Vec::new(),
            expected_parent_identity: None,
        })
        .await
        .expect("lookup root")
        .into_inner()
        .attr
        .expect("root attr")
        .identity
        .expect("root identity");
    let created = raw
        .create(OwnerCreateRequest {
            access: Some(access.clone()),
            path: b"oversize-read.bin".to_vec(),
            flags: libc::O_RDWR as u32,
            mode: 0o644,
            expected_parent: Some(root),
            caller: Some(OwnerCaller {
                uid: fixture.ctx.uid,
                gid: fixture.ctx.gid,
                pid: fixture.ctx.pid,
                umask: fixture.ctx.umask,
                supplementary_gids: fixture.ctx.supplementary_gids.clone(),
            }),
            kill_suidgid: false,
        })
        .await
        .expect("create oversize target")
        .into_inner();
    let before = *fixture.meta.validate_calls.lock().unwrap();
    let status = raw
        .read(OwnerReadRequest {
            access: Some(access.clone()),
            handle: created.handle.clone(),
            offset: 0,
            length: u32::MAX,
            plane: Some(DataPlane {
                transfer: DataTransfer::RdmaOneSided.into(),
                rdma_session_id: 1,
                buffer_offset: 0,
            }),
        })
        .await
        .expect_err("oversize Owner RDMA read must reject before handler allocation");
    assert!(
        status.message().contains("transfer exceeds")
            || status.message().contains("1MiB")
            || status.message().contains("invalid"),
        "unexpected oversize read status: {status:?}"
    );
    assert_eq!(
        *fixture.meta.validate_calls.lock().unwrap(),
        before,
        "oversize read must fail before handler authority validation and allocation"
    );
    raw.release(OwnerReleaseRequest {
        access: Some(access),
        handle: created.handle,
    })
    .await
    .expect("release oversize handle");
    fixture.server.abort();
}

#[cfg(feature = "rdma")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ownerpeerclient_rdma_without_device_ignores_forbidden_prefetch() {
    let handler = Arc::new(PrefetchDespiteDisableHandler::default());
    let (channel, server) = prefetch_despite_disable_server(handler.clone()).await;
    let result = tokio::task::spawn_blocking({
        let grant = prefetch_test_grant();
        move || -> afs_error::Result<()> {
            let client = owner_files_client_from_channel(channel).with_data_transport(
                ClientDataMode::Rdma,
                None,
                Duration::from_millis(250),
            );
            let (file, attrs) =
                client.open(&grant, OsStr::new("prefetch.txt"), libc::O_RDONLY, None)?;
            assert_eq!(attrs.size, 6);
            let mut buf = [0_u8; 6];
            let error = client
                .read(&grant, &file, 0, &mut buf)
                .expect_err("RDMA-required read without a device must not use prefetched bytes");
            assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
            assert_eq!(buf, [0_u8; 6]);
            Ok(())
        }
    })
    .await
    .expect("RDMA no-device prefetch regression task");
    result.expect("RDMA no-device prefetch regression assertions");
    assert_eq!(
        handler.open_disable_prefetch.lock().unwrap().as_slice(),
        &[true]
    );
    assert_eq!(*handler.read_calls.lock().unwrap(), 0);
    server.abort();
}

#[cfg(feature = "rdma")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ownerpeerclient_rdma_without_device_rejects_read_write_without_mutation() {
    let fixture = owner_rdma_fixture(None, "job-client-rdma-none").await;
    let result = tokio::task::spawn_blocking({
        let channel = fixture.channel.clone();
        let grant = fixture.grant.clone();
        let ctx = fixture.ctx.clone();
        move || -> afs_error::Result<()> {
            let setup = owner_files_client_from_channel(channel.clone());
            let rdma = owner_files_client_from_channel(channel).with_data_transport(
                ClientDataMode::Rdma,
                None,
                Duration::from_millis(250),
            );
            let root_parent = setup.lookup(&grant, OsStr::new(""), None)?.identity;
            let created = setup.create(
                &ctx,
                &grant,
                OsStr::new("client-rdma-none.bin"),
                libc::O_RDWR,
                0o644,
                &root_parent,
            )?;

            let write_error = rdma
                .write(&grant, &created.file, 0, b"bad")
                .expect_err("strict OwnerPeerClient RDMA without device must reject write");
            assert_eq!(write_error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
            let after_write = setup.getattr(
                &grant,
                OsStr::new("client-rdma-none.bin"),
                Some(&created.entry.identity),
                Some(&created.file),
            )?;
            assert_eq!(after_write.attributes.size, 0);

            let mut one = [0_u8; 1];
            let read_error = rdma
                .read(&grant, &created.file, 0, &mut one)
                .expect_err("strict OwnerPeerClient RDMA without device must reject read");
            assert_eq!(read_error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
            assert_eq!(setup.read(&grant, &created.file, 0, &mut one)?, 0);
            setup.release(&grant, created.file)?;
            Ok(())
        }
    })
    .await
    .expect("strict no-device client ops");
    result.expect("strict no-device client assertions");
    fixture.server.abort();
}

#[cfg(feature = "rdma")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ownerpeerclient_auto_without_device_falls_back_before_rdma_dispatch() {
    let fixture = owner_rdma_fixture(None, "job-client-auto-none").await;
    let registry = afs_metrics::Registry::new();
    let metrics = afs::node::rpc::OwnerRpcMetrics::register(&registry).expect("Owner metrics");
    let result = tokio::task::spawn_blocking({
        let channel = fixture.channel.clone();
        let grant = fixture.grant.clone();
        let ctx = fixture.ctx.clone();
        let metrics = metrics.clone();
        let runtime = tokio::runtime::Handle::current();
        move || -> afs_error::Result<()> {
            let setup = owner_files_client_from_channel(channel.clone());
            let auto =
                afs::node::rpc::peer::owner_files_client_from_channel_with_runtime_and_metrics(
                    channel,
                    runtime,
                    Some(metrics),
                )
                .with_data_transport(
                    ClientDataMode::Auto,
                    None,
                    Duration::from_millis(250),
                );
            let root_parent = setup.lookup(&grant, OsStr::new(""), None)?.identity;
            let created = setup.create(
                &ctx,
                &grant,
                OsStr::new("client-auto-fallback.bin"),
                libc::O_RDWR,
                0o644,
                &root_parent,
            )?;

            assert_eq!(auto.write(&grant, &created.file, 0, b"auto")?, 4);
            let after_write = setup.getattr(
                &grant,
                OsStr::new("client-auto-fallback.bin"),
                Some(&created.entry.identity),
                Some(&created.file),
            )?;
            assert_eq!(after_write.attributes.size, 4);

            let mut data = [0_u8; 4];
            assert_eq!(auto.read(&grant, &created.file, 0, &mut data)?, 4);
            assert_eq!(&data, b"auto");
            setup.release(&grant, created.file)?;
            Ok(())
        }
    })
    .await
    .expect("auto no-device client ops");
    result.expect("auto no-device client assertions");
    assert_eq!(owner_payload_bytes(&registry, "client", "write", "grpc"), 4);
    assert_eq!(owner_payload_bytes(&registry, "client", "read", "grpc"), 4);
    assert_eq!(owner_payload_bytes(&registry, "client", "write", "rdma"), 0);
    assert_eq!(owner_payload_bytes(&registry, "client", "read", "rdma"), 0);
    fixture.server.abort();
}

#[cfg(feature = "rdma")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working RXE/RDMA device"]
async fn ownerpeerclient_rdma_large_write_fsync_cold_read_roundtrip_preserves_payload() {
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let fixture = owner_rdma_fixture(Some(device.clone()), "job-client-rdma").await;
    let registry = afs_metrics::Registry::new();
    let metrics = afs::node::rpc::OwnerRpcMetrics::register(&registry).expect("Owner metrics");
    let payload = owner_payload(4 * 1024 * 1024 + 17);
    let expected_hash = blake3::hash(&payload);
    let result = tokio::task::spawn_blocking({
        let channel = fixture.channel.clone();
        let grant = fixture.grant.clone();
        let ctx = fixture.ctx.clone();
        let payload = payload.clone();
        let metrics = metrics.clone();
        let runtime = tokio::runtime::Handle::current();
        move || -> afs_error::Result<(usize, usize, Vec<u8>)> {
            let client =
                afs::node::rpc::peer::owner_files_client_from_channel_with_runtime_and_metrics(
                    channel,
                    runtime,
                    Some(metrics),
                )
                .with_data_transport(
                    ClientDataMode::Rdma,
                    Some(device),
                    Duration::from_secs(10),
                );
            let root_parent = client.lookup(&grant, OsStr::new(""), None)?.identity;
            let created = client.create(
                &ctx,
                &grant,
                OsStr::new("client-rdma-large.bin"),
                libc::O_RDWR,
                0o644,
                &root_parent,
            )?;
            let window = 1024 * 1024;
            let mut write_windows = 0usize;
            for (index, chunk) in payload.chunks(window).enumerate() {
                assert_eq!(
                    client.write(&grant, &created.file, (index * window) as u64, chunk)?,
                    chunk.len()
                );
                write_windows += 1;
            }
            client.fsync(&grant, &created.file, false)?;
            client.release(&grant, created.file.clone())?;

            let (cold, _attrs) = client.open(
                &grant,
                OsStr::new("client-rdma-large.bin"),
                libc::O_RDONLY,
                Some(&created.entry.identity),
            )?;
            let mut read_back = vec![0_u8; payload.len()];
            let mut read = 0usize;
            let mut read_windows = 0usize;
            while read < read_back.len() {
                let end = read.saturating_add(window).min(read_back.len());
                let count = client.read(&grant, &cold, read as u64, &mut read_back[read..end])?;
                assert_ne!(
                    count, 0,
                    "OwnerPeerClient RDMA read hit EOF before full payload"
                );
                read += count;
                read_windows += 1;
            }
            let mut eof = [0_u8; 1];
            assert_eq!(
                client.read(&grant, &cold, payload.len() as u64, &mut eof)?,
                0
            );
            client.release(&grant, cold)?;
            Ok((write_windows, read_windows, read_back))
        }
    })
    .await
    .expect("production RDMA client ops")
    .expect("production RDMA client assertions");
    let expected_windows = payload.chunks(1024 * 1024).count();
    assert_eq!(result.0, expected_windows);
    assert_eq!(result.1, expected_windows);
    assert_eq!(blake3::hash(&result.2), expected_hash);
    assert_eq!(result.2, payload);
    assert_eq!(
        owner_payload_bytes(&registry, "client", "write", "rdma"),
        payload.len() as u64
    );
    assert_eq!(
        owner_payload_bytes(&registry, "client", "read", "rdma"),
        payload.len() as u64
    );
    assert_eq!(owner_payload_bytes(&registry, "client", "read", "grpc"), 0);
    assert_eq!(owner_payload_bytes(&registry, "client", "write", "grpc"), 0);
    fixture.server.abort();
}

#[cfg(feature = "rdma")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real RXE and external GDB stop at posted OwnerFiles READ4096 deadline point"]
async fn ownerpeerclient_posted_rdma_deadline_has_unknown_outcome() {
    let device = rdma_device().expect("explicit RXE tests require AFS_TEST_RDMA_DEVICE");
    let checkpoint = std::path::PathBuf::from(
        std::env::var_os("AFS_TEST_RDMA_CHECKPOINT").expect("external GDB checkpoint directory"),
    );
    save_rdma_resources(&checkpoint, "baseline");

    let root_name = "job-owner-deadline";
    let file_name = "posted-deadline.bin";
    let fixture = owner_rdma_fixture(Some(device.clone()), root_name).await;
    let file_path = owner_fixture_file_path(&fixture, file_name);
    let payload = owner_payload(4096);
    let write = tokio::task::spawn_blocking({
        let channel = fixture.channel.clone();
        let grant = fixture.grant.clone();
        let ctx = fixture.ctx.clone();
        let metrics = fixture.metrics.clone();
        let runtime = tokio::runtime::Handle::current();
        let payload = payload.clone();
        move || -> afs_error::Result<usize> {
            let client =
                afs::node::rpc::peer::owner_files_client_from_channel_with_runtime_and_metrics(
                    channel,
                    runtime,
                    Some(metrics),
                )
                .with_data_transport(
                    ClientDataMode::Auto,
                    Some(device),
                    Duration::from_secs(4),
                );
            let root_parent = client.lookup(&grant, OsStr::new(""), None)?.identity;
            let created = client.create(
                &ctx,
                &grant,
                OsStr::new(file_name),
                libc::O_RDWR,
                0o644,
                &root_parent,
            )?;
            client.write(&grant, &created.file, 0, &payload)
        }
    });

    wait_for_rdma_checkpoint(&checkpoint.join("posted")).await;
    save_rdma_resources(&checkpoint, "connected");
    let identity = PeerSessionIdentity::for_owner(
        "node-b".to_owned(),
        owner_presented_access_for(&fixture.grant),
    )
    .expect("Owner RDMA identity");
    let session = fixture
        .rdma_registry
        .session_for(1, &identity)
        .await
        .expect("bound Owner RDMA session");
    let retained_endpoint = Arc::downgrade(&session.endpoint);
    drop(session);

    let error = tokio::time::timeout(Duration::from_secs(10), write)
        .await
        .expect("posted client request must reach its own deadline")
        .expect("spawn_blocking client task must not panic")
        .expect_err("posted OwnerPeerClient write must report an unknown deadline outcome");
    std::fs::write(
        checkpoint.join("caller-error.json"),
        serde_json::to_vec(&serde_json::json!({
            "code": error.code().raw(),
            "kind": format!("{:?}", error.kind()),
            "message": error.message(),
        }))
        .unwrap(),
    )
    .expect("save the actual framework deadline classification");
    assert_owner_deadline(&error);
    wait_for_owner_rdma_lookup_stale(&fixture.rdma_registry, 1, &identity).await;
    {
        let endpoint = retained_endpoint
            .upgrade()
            .expect("blocked worker retains server endpoint");
        assert!(
            endpoint.try_lock().is_err(),
            "native worker still owns the server endpoint while paused"
        );
    }
    assert_eq!(
        std::fs::read(&file_path).expect("created file remains readable while paused"),
        Vec::<u8>::new()
    );
    assert_eq!(
        owner_payload_bytes(&fixture.metrics_registry, "client", "write", "rdma"),
        0
    );
    assert_eq!(
        owner_payload_bytes(&fixture.metrics_registry, "client", "write", "grpc"),
        0
    );
    assert_eq!(
        owner_payload_bytes(&fixture.metrics_registry, "server", "write", "grpc"),
        0
    );
    save_rdma_resources(&checkpoint, "closed-paused");
    eprintln!(
        "AFS_OWNER_DEADLINE caller=TIMEOUT lookup=STALE server=RETAINED file=EMPTY replay=ABSENT"
    );

    std::fs::write(checkpoint.join("resume"), b"resume").expect("resume native worker");
    wait_for_endpoint_release(retained_endpoint).await;
    assert_eq!(
        std::fs::read(&file_path).expect("admitted write content after resume"),
        payload
    );
    assert_eq!(
        owner_payload_bytes(&fixture.metrics_registry, "client", "write", "rdma"),
        0
    );
    assert_eq!(
        owner_payload_bytes(&fixture.metrics_registry, "client", "write", "grpc"),
        0
    );
    assert_eq!(
        owner_payload_bytes(&fixture.metrics_registry, "server", "write", "rdma"),
        0,
        "the timed-out RPC has no observed successful reply, even if its worker writes the file"
    );
    assert_eq!(
        owner_payload_bytes(&fixture.metrics_registry, "server", "write", "grpc"),
        0
    );
    save_rdma_resources(&checkpoint, "drained");
    eprintln!("AFS_OWNER_DEADLINE drain=COMPLETE content=EXACT endpoint=RELEASED replay=ABSENT");
    fixture.server.abort();
    let _ = fixture.server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ownerfiles_rejects_rdma_plane_without_negotiated_owner_session_before_write() {
    let temp = tempfile::tempdir().expect("tempdir");
    let disk = Arc::new(LocalFs::open(temp.path()).expect("localfs"));
    let meta = Arc::new(ContractMeta::default());
    let roots = Arc::new(RootManager::new(
        "node-a".to_owned(),
        "session-a".to_owned(),
        meta.clone(),
        disk.clone(),
    ));
    let fs = Arc::new(OwnerFs::new_local(roots, disk));
    let ctx = RequestContext {
        uid: temp.path().metadata().expect("fixture metadata").uid(),
        gid: temp.path().metadata().expect("fixture metadata").gid(),
        pid: 42,
        umask: 0,
        supplementary_gids: Vec::new(),
    };
    let owner_root = BackendInode { value: 1 };
    fs.mkdir(&ctx, owner_root, OsStr::new("job-plane"), 0o755)
        .expect("create home root");
    let root_id = root_id_from_name(OsStr::new("job-plane")).expect("root id");
    let grant = meta
        .acquire_root(&root_id, RootRight::Write)
        .expect("remote grant");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let executor = fs.peer_executor().expect("peer executor");
    let control = afs::node::rpc::control::NodeControlService::new(
        afs::node::rpc::control::RdmaSessionRegistry::new(None),
    )
    .with_owner_locks(executor.clone(), Arc::new(RequireMtlsNodeB))
    .into_server();
    let handler = make_owner_files_handler(executor);
    let server_tls = ServerTlsConfig::new()
        .client_ca_root(Certificate::from_pem(CA_PEM))
        .identity(Identity::from_pem(SERVER_CERT_PEM, SERVER_KEY_PEM));
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls)
            .expect("server tls")
            .add_service(control)
            .add_service(make_owner_files_server_with_handler(
                handler,
                Arc::new(RequireMtlsNodeB),
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("owner files server");
    });

    let client_tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(CA_PEM))
        .identity(Identity::from_pem(CLIENT_CERT_PEM, CLIENT_KEY_PEM))
        .domain_name("localhost");
    let channel: Channel = Endpoint::from_shared(endpoint)
        .expect("endpoint")
        .timeout(Duration::from_secs(5))
        .tls_config(client_tls)
        .expect("client tls")
        .connect()
        .await
        .expect("connect");
    let mut raw = OwnerFilesClient::new(channel);
    let access = root_access_for(&grant);
    let root = raw
        .lookup(OwnerLookupRequest {
            access: Some(access.clone()),
            path: Vec::new(),
            expected_parent_identity: None,
        })
        .await
        .expect("lookup root")
        .into_inner()
        .attr
        .expect("root attr")
        .identity
        .expect("root identity");
    let created = raw
        .create(OwnerCreateRequest {
            access: Some(access.clone()),
            path: b"rdma-plane-reject.bin".to_vec(),
            flags: libc::O_RDWR as u32,
            mode: 0o644,
            expected_parent: Some(root),
            caller: Some(OwnerCaller {
                uid: ctx.uid,
                gid: ctx.gid,
                pid: ctx.pid,
                umask: ctx.umask,
                supplementary_gids: ctx.supplementary_gids.clone(),
            }),
            kill_suidgid: false,
        })
        .await
        .expect("create file")
        .into_inner();
    let handle = created.handle.expect("created handle");
    let identity = created
        .attr
        .as_ref()
        .and_then(|attr| attr.identity.clone())
        .expect("created identity");

    let status = raw
        .write(OwnerWriteRequest {
            access: Some(access.clone()),
            handle: Some(handle.clone()),
            offset: 0,
            data: b"bad".to_vec(),
            length: 3,
            plane: Some(DataPlane {
                transfer: DataTransfer::RdmaOneSided.into(),
                rdma_session_id: 0,
                buffer_offset: u64::MAX,
            }),
            kill_suidgid: false,
            ..Default::default()
        })
        .await
        .expect_err("unnegotiated Owner RDMA data plane must fail before write");
    let status_message = status.message().to_ascii_lowercase();
    assert!(
        status_message.contains("rdma")
            || status_message.contains("data plane")
            || status_message.contains("session"),
        "unexpected RDMA rejection status: {status:?}"
    );

    let attr = raw
        .get_attr(OwnerGetAttrRequest {
            access: Some(access.clone()),
            path: b"rdma-plane-reject.bin".to_vec(),
            expected_file_identity: Some(identity),
            handle: Some(handle.clone()),
        })
        .await
        .expect("getattr after rejected write")
        .into_inner()
        .attr
        .expect("attr after rejected write");
    assert_eq!(attr.size, 0);
    let read = raw
        .read(OwnerReadRequest {
            access: Some(access.clone()),
            handle: Some(handle.clone()),
            offset: 0,
            length: 1,
            plane: Some(DataPlane {
                transfer: DataTransfer::GrpcInline.into(),
                rdma_session_id: 0,
                buffer_offset: 0,
            }),
        })
        .await
        .expect("read after rejected write")
        .into_inner();
    assert_eq!(read.read, 0);
    assert!(read.data.is_empty());
    raw.release(OwnerReleaseRequest {
        access: Some(access),
        handle: Some(handle),
    })
    .await
    .expect("release handle");
    assert_eq!(*meta.validate_calls.lock().unwrap(), 1);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_ownerfiles_grpc_roundtrip_uses_real_ownerfs_backend() {
    let temp = tempfile::tempdir().expect("tempdir");
    let disk = Arc::new(LocalFs::open(temp.path()).expect("localfs"));
    let meta = Arc::new(ContractMeta::default());
    let roots = Arc::new(RootManager::new(
        "node-a".to_owned(),
        "session-a".to_owned(),
        meta.clone(),
        disk.clone(),
    ));
    let fs = Arc::new(OwnerFs::new_local(roots, disk));
    let ctx = RequestContext {
        uid: temp.path().metadata().expect("fixture metadata").uid(),
        gid: temp.path().metadata().expect("fixture metadata").gid(),
        pid: 42,
        umask: 0,
        supplementary_gids: Vec::new(),
    };
    let owner_root = BackendInode { value: 1 };
    fs.mkdir(&ctx, owner_root, OsStr::new("job-42"), 0o755)
        .expect("create home root");
    let root_id = root_id_from_name(OsStr::new("job-42")).expect("root id");
    let grant = meta
        .acquire_root(&root_id, RootRight::Write)
        .expect("remote grant");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let endpoint = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let executor = fs.peer_executor().expect("peer executor");
    let control = afs::node::rpc::control::NodeControlService::new(
        afs::node::rpc::control::RdmaSessionRegistry::new(None),
    )
    .with_owner_locks(executor.clone(), Arc::new(RequireMtlsNodeB))
    .into_server();
    let handler = make_owner_files_handler(executor);
    let server_tls = ServerTlsConfig::new()
        .client_ca_root(Certificate::from_pem(CA_PEM))
        .identity(Identity::from_pem(SERVER_CERT_PEM, SERVER_KEY_PEM));
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls)
            .expect("server tls")
            .add_service(control)
            .add_service(make_owner_files_server_with_handler(
                handler,
                Arc::new(RequireMtlsNodeB),
            ))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("owner files server");
    });

    let ca = temp.path().join("ca.pem");
    let cert = temp.path().join("client.pem");
    let key = temp.path().join("client-key.pem");
    std::fs::write(&ca, CA_PEM).unwrap();
    std::fs::write(&cert, CLIENT_CERT_PEM).unwrap();
    std::fs::write(&key, CLIENT_KEY_PEM).unwrap();
    let peers = afs::node::rpc::peer::PeerConnectionPool::new(
        afs_transport::GrpcConfig::default(),
        afs_transport::TlsConfig::MutualTls {
            ca_certificate: ca,
            identity_certificate: cert,
            identity_private_key: key,
            server_name: "localhost".into(),
        },
        4,
    )
    .unwrap();
    let channel = peers
        .owner_files_channel("node-a", 1, &endpoint)
        .await
        .unwrap();
    let long_wait_channel = peers
        .long_wait_channel("node-a", 1, &endpoint)
        .await
        .unwrap();
    let missing_parent_status = OwnerFilesClient::new(channel.clone())
        .lookup(OwnerLookupRequest {
            access: Some(root_access_for(&grant)),
            path: b"missing-parent.txt".to_vec(),
            expected_parent_identity: None,
        })
        .await
        .expect_err("OwnerFiles.Lookup without non-root expected_parent_identity must fail");
    assert!(
        missing_parent_status
            .message()
            .contains("OwnerLookupRequest missing expected_parent_identity"),
        "unexpected status for missing expected_parent_identity: {missing_parent_status:?}"
    );
    let missing_parent_status = OwnerFilesClient::new(channel.clone())
        .create(OwnerCreateRequest {
            access: Some(root_access_for(&grant)),
            path: b"missing-parent.txt".to_vec(),
            flags: libc::O_RDWR as u32,
            mode: 0o644,
            expected_parent: None,
            caller: Some(OwnerCaller {
                uid: ctx.uid,
                gid: ctx.gid,
                pid: ctx.pid,
                umask: ctx.umask,
                supplementary_gids: ctx.supplementary_gids.clone(),
            }),
            kill_suidgid: false,
        })
        .await
        .expect_err("OwnerFiles.Create without expected_parent must fail");
    assert!(
        missing_parent_status
            .message()
            .contains("OwnerCreateRequest missing expected_parent"),
        "unexpected status for missing expected_parent: {missing_parent_status:?}"
    );
    // Caller-less destructive requests fail before any namespace mutation,
    // even when all identity fields are supplied.
    let mut raw_missing_caller = OwnerFilesClient::new(channel.clone());
    let parent = Some(FileIdentity {
        opaque: b"identity-present".to_vec(),
    });
    let status = raw_missing_caller
        .unlink(OwnerUnlinkRequest {
            access: Some(root_access_for(&grant)),
            path: b"absent".to_vec(),
            expected_file_identity: None,
            expected_parent: parent.clone(),
            caller: None,
        })
        .await
        .expect_err("unlink requires caller");
    assert!(status.message().contains("missing caller"));
    let status = raw_missing_caller
        .rmdir(OwnerRmdirRequest {
            access: Some(root_access_for(&grant)),
            path: b"absent".to_vec(),
            expected_file_identity: None,
            expected_parent: parent.clone(),
            caller: None,
        })
        .await
        .expect_err("rmdir requires caller");
    assert!(status.message().contains("missing caller"));
    let status = raw_missing_caller
        .rename(OwnerRenameRequest {
            access: Some(root_access_for(&grant)),
            old_path: b"absent".to_vec(),
            new_path: b"new".to_vec(),
            expected_old_identity: None,
            expected_new_identity: None,
            flags: 0,
            expected_old_parent: parent.clone(),
            expected_new_parent: parent,
            caller: None,
        })
        .await
        .expect_err("rename requires caller");
    assert!(status.message().contains("missing caller"));
    let raw_channel = channel.clone();
    let raw_grant = grant.clone();
    let client = std::thread::spawn(move || {
        owner_files_client_from_channel(channel).with_long_wait_channel(long_wait_channel)
    })
    .join()
    .expect("OwnerFiles client construction must not require a Tokio reactor");

    tokio::task::spawn_blocking(move || -> afs_error::Result<()> {
        let root_parent = client.lookup(&grant, OsStr::new(""), None)?.identity;
        let created = client.create(
            &ctx,
            &grant,
            OsStr::new("log.txt"),
            libc::O_RDWR,
            0o644,
            &root_parent,
        )?;
        // Exercise a full-size payload across the production pool's mTLS profile.
        let payload = vec![0x9d; 1024 * 1024];
        assert_eq!(
            client.write(&grant, &created.file, 0, &payload)?,
            payload.len()
        );
        let mut readback = vec![0; payload.len()];
        assert_eq!(
            client.read(&grant, &created.file, 0, &mut readback)?,
            payload.len()
        );
        assert_eq!(readback, payload);
        let client = Arc::new(client);
        let held = owner_test_lock(
            "remote-mount",
            10,
            afs::node::vfs::types::FileLockType::Write,
        );
        client.setlk(&grant, &created.file, held.clone(), None)?;
        let home_root = fs.lookup(&ctx, owner_root, OsStr::new("job-42"))?;
        let home_file = fs.lookup(&ctx, home_root.inode, OsStr::new("log.txt"))?;
        let home_handle = fs.open(&ctx, home_file.inode, libc::O_RDWR)?;
        // Matching raw strings/kernel IDs from local and peer ingress still
        // identify distinct owners, and both meet the same Home inode table.
        assert!(
            fs.setlk(
                &ctx,
                home_file.inode,
                home_handle,
                owner_test_lock(
                    "remote-mount",
                    10,
                    afs::node::vfs::types::FileLockType::Write
                ),
                None
            )
            .is_err()
        );
        fs.release(&ctx, home_handle)?;
        let mut forged = created.file.clone();
        forged.identity.0 = b"wrong-file-identity".to_vec();
        assert!(client.getlk(&grant, &forged, held.clone()).is_err());
        let other = owner_test_lock(
            "other-mount",
            11,
            afs::node::vfs::types::FileLockType::Write,
        );
        assert!(
            client
                .setlk(&grant, &created.file, other.clone(), None)
                .is_err()
        );
        assert!(
            client
                .getlk(&grant, &created.file, other.clone())?
                .is_some()
        );
        let waiter = afs::node::vfs::locks::LockWaiterId {
            ingress_session_id: "other-mount".into(),
            request_id: 9001,
        };
        let waiting_client = client.clone();
        let waiting_grant = grant.clone();
        let waiting_file = created.file.clone();
        let waiting_id = waiter.clone();
        let (send, receive) = std::sync::mpsc::channel();
        let wait_thread = std::thread::spawn(move || {
            send.send(waiting_client.setlk(&waiting_grant, &waiting_file, other, Some(waiting_id)))
                .unwrap();
        });
        // The ordinary Endpoint has a 5 second timeout. SETLKW uses the
        // separately authenticated no-deadline channel and must survive it.
        assert!(receive.recv_timeout(Duration::from_millis(5200)).is_err());
        let mut unlock = held;
        unlock.lock_type = afs::node::vfs::types::FileLockType::Unlock;
        client.setlk(&grant, &created.file, unlock, None)?;
        receive
            .recv_timeout(Duration::from_secs(3))
            .expect("long waiter completed")?;
        wait_thread.join().unwrap();
        assert_eq!(
            client.cancel_lock_wait(&grant, waiter)?,
            afs::node::vfs::locks::LockWaiterOutcome::Unknown
        );
        client.release_locks(
            &grant,
            &created.file,
            owner_test_lock(
                "other-mount",
                11,
                afs::node::vfs::types::FileLockType::Write,
            )
            .owner,
            afs::node::vfs::types::ReleaseKind::PosixOwner,
        )?;
        client.release_lock_session(&grant, "remote-mount")?;
        client.release_lock_session(&grant, "other-mount")?;
        let denied = RequestContext {
            uid: if ctx.uid == 0 { 1000 } else { ctx.uid + 1 },
            gid: if ctx.gid == 0 { 1000 } else { ctx.gid + 1 },
            supplementary_gids: Vec::new(),
            ..ctx.clone()
        };
        let directory = client.mkdir(
            &ctx,
            &grant,
            OsStr::new("protected-dir"),
            0o700,
            &root_parent,
        )?;
        assert_eq!(
            client
                .unlink(
                    &denied,
                    &grant,
                    OsStr::new("log.txt"),
                    Some(&created.entry.identity),
                    &root_parent
                )
                .unwrap_err()
                .code(),
            afs_error::IO_PERMISSION_DENIED
        );
        assert_eq!(
            client
                .rmdir(
                    &denied,
                    &grant,
                    OsStr::new("protected-dir"),
                    Some(&directory.identity),
                    &root_parent
                )
                .unwrap_err()
                .code(),
            afs_error::IO_PERMISSION_DENIED
        );
        assert_eq!(
            client
                .rename(
                    &denied,
                    &grant,
                    OsStr::new("log.txt"),
                    OsStr::new("denied.txt"),
                    Some(&created.entry.identity),
                    None,
                    &root_parent,
                    &root_parent,
                    RenameFlags(0)
                )
                .unwrap_err()
                .code(),
            afs_error::IO_PERMISSION_DENIED
        );
        client.rmdir(
            &ctx,
            &grant,
            OsStr::new("protected-dir"),
            Some(&directory.identity),
            &root_parent,
        )?;
        assert_eq!(client.write(&grant, &created.file, 0, b"AAAA")?, 4);

        // The test Meta intentionally grants a second process session for
        // node-b. A valid root grant still must not borrow the first session's
        // opened Home handle, even though the opaque numeric ID is known.
        let mut other_session = grant.clone();
        other_session.session_id = "session-b-restarted".to_owned();
        let error = client
            .write(&other_session, &created.file, 0, b"XXXX")
            .expect_err("another peer session must not borrow an open handle");
        assert_eq!(error.code(), afs_error::NODE_OWNER_STALE_HANDLE);

        let mut buf = [0_u8; 4];
        assert_eq!(client.read(&grant, &created.file, 0, &mut buf)?, 4);
        assert_eq!(&buf, b"AAAA");

        client.rename(
            &ctx,
            &grant,
            OsStr::new("log.txt"),
            OsStr::new("renamed.txt"),
            Some(&created.entry.identity),
            None,
            &root_parent,
            &root_parent,
            RenameFlags(0),
        )?;
        client.unlink(
            &ctx,
            &grant,
            OsStr::new("renamed.txt"),
            Some(&created.entry.identity),
            &root_parent,
        )?;

        let recreated = client.create(
            &ctx,
            &grant,
            OsStr::new("renamed.txt"),
            libc::O_RDWR,
            0o644,
            &root_parent,
        )?;
        assert_eq!(client.write(&grant, &recreated.file, 0, b"BBBB")?, 4);

        let mut old = [0_u8; 4];
        assert_eq!(client.read(&grant, &created.file, 0, &mut old)?, 4);
        assert_eq!(&old, b"AAAA");
        let mut new = [0_u8; 4];
        assert_eq!(client.read(&grant, &recreated.file, 0, &mut new)?, 4);
        assert_eq!(&new, b"BBBB");

        client.release(&grant, created.file.clone())?;
        let error = client
            .read(&grant, &created.file, 0, &mut old)
            .expect_err("released remote handle must be stale");
        assert_eq!(error.code(), afs_error::NODE_OWNER_STALE_HANDLE);
        client.release(&grant, recreated.file)?;
        Ok(())
    })
    .await
    .expect("blocking remote ops")
    .expect("remote ops");

    // Prefetch is an OwnerFs policy, not a Proto adapter policy. A new
    // read-only OPEN observes the final contents; writable OPEN never embeds
    // bytes and must continue through the normal file handle path.
    let mut raw = OwnerFilesClient::new(raw_channel);
    for (flags, expected_prefetch) in [
        (libc::O_RDONLY as u32, Some(b"BBBB".to_vec())),
        (libc::O_RDWR as u32, None),
    ] {
        let opened = raw
            .open(OwnerOpenRequest {
                access: Some(root_access_for(&raw_grant)),
                path: b"renamed.txt".to_vec(),
                flags,
                mode: 0,
                expected_file_identity: None,
                kill_suidgid: false,
                ..Default::default()
            })
            .await
            .expect("open for prefetch contract")
            .into_inner();
        assert_eq!(opened.prefetched_data, expected_prefetch);
        raw.release(OwnerReleaseRequest {
            access: Some(root_access_for(&raw_grant)),
            handle: opened.handle,
        })
        .await
        .expect("release raw handle");
    }

    assert_eq!(*meta.validate_calls.lock().unwrap(), 2);
    server.abort();
}

fn root_access_for(grant: &RootGrant) -> RootAccess {
    RootAccess {
        root_id: grant.id.0.clone(),
        root_epoch: grant.epoch,
        access_generation: grant.access_generation,
        holder_node_id: grant.holder_node_id.clone(),
        home_node_id: grant.home_node_id.clone(),
        session_id: grant.session_id.clone(),
        fencing_token: grant.fencing_token.clone(),
        home_session_id: grant.home_session_id.clone(),
    }
}

#[test]
fn mtls_authenticator_is_available_for_exact_der_mapping() {
    assert!(MtlsPeerAuthenticator::new(vec![("node-b".to_owned(), vec![1, 2, 3])]).is_ok());
}

fn owner_test_lock(
    scope: &str,
    kernel_owner: u64,
    lock_type: afs::node::vfs::types::FileLockType,
) -> afs::node::vfs::locks::LockRequest {
    afs::node::vfs::locks::LockRequest {
        kind: afs::node::vfs::types::FileLockKind::Posix,
        owner: afs::node::vfs::types::FileLockOwner {
            ingress_session_id: scope.into(),
            kernel_owner,
        },
        pid: kernel_owner as u32,
        range: afs::node::vfs::types::FileLockRange { start: 0, end: 99 },
        lock_type,
    }
}
