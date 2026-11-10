use io_uring::{IoUring, opcode, squeue, types};
use libublk::sys;
use std::{fs::OpenOptions, io, mem::offset_of, os::fd::AsRawFd, thread, time::Duration};
use tokio::{sync::watch, time::Instant};

const CONTROL: u64 = 1;
const CANCEL: u64 = 2;
const CHECK_INTERVAL_MS: i32 = 20;

#[derive(Clone, Copy)]
pub(super) enum Operation {
    Start,
    BeginRecovery,
    EndRecovery,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::BeginRecovery => "begin_recovery",
            Self::EndRecovery => "end_recovery",
        }
    }

    fn opcode(self, features: u64) -> u32 {
        let encoded = match self {
            Self::Start => sys::UBLK_U_CMD_START_DEV,
            Self::BeginRecovery => sys::UBLK_U_CMD_START_USER_RECOVERY,
            Self::EndRecovery => sys::UBLK_U_CMD_END_USER_RECOVERY,
        };
        if features & sys::UBLK_F_CMD_IOCTL_ENCODE as u64 != 0 {
            encoded
        } else {
            encoded & 0xff
        }
    }
}

pub(super) struct Control {
    ring: IoUring<squeue::Entry128>,
    file: std::fs::File,
    id: u32,
    features: u64,
}

impl Control {
    pub(super) fn new(id: u32, features: Option<u64>) -> io::Result<Self> {
        Ok(Self {
            ring: IoUring::builder().build(4)?,
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .open("/dev/ublk-control")?,
            id,
            features: features.unwrap_or_default(),
        })
    }

    pub(super) fn execute(
        &mut self,
        op: Operation,
        deadline: Instant,
        queue_failed: Option<&watch::Receiver<bool>>,
    ) -> io::Result<()> {
        if let Some(error) = cancellation(deadline, queue_failed) {
            return Err(error);
        }
        let mut bytes = [0; 80];
        let dev_id = offset_of!(sys::ublksrv_ctrl_cmd, dev_id);
        let queue_id = offset_of!(sys::ublksrv_ctrl_cmd, queue_id);
        let data = offset_of!(sys::ublksrv_ctrl_cmd, data);
        bytes[dev_id..dev_id + 4].copy_from_slice(&self.id.to_ne_bytes());
        bytes[queue_id..queue_id + 2].copy_from_slice(&u16::MAX.to_ne_bytes());
        if !matches!(op, Operation::BeginRecovery) {
            bytes[data..data + 8].copy_from_slice(&u64::from(std::process::id()).to_ne_bytes());
        }
        let command =
            opcode::UringCmd80::new(types::Fd(self.file.as_raw_fd()), op.opcode(self.features))
                .cmd(bytes)
                .build()
                .flags(squeue::Flags::ASYNC)
                .user_data(CONTROL);
        self.push(command);
        submit(&self.ring)?;

        let mut reason = None;
        loop {
            let completion = self
                .ring
                .completion()
                .find(|cqe| cqe.user_data() == CONTROL);
            if let Some(cqe) = completion {
                let result = cqe.result();
                if let Some(error) = reason.or_else(|| cancellation(deadline, queue_failed)) {
                    afs_logging::warn!("overlaybd.control_drained"; "device_id" => self.id,
                        "command" => op.name(), "result" => result, "reason" => error.to_string());
                    return Err(error);
                }
                return if result < 0 {
                    Err(io::Error::from_raw_os_error(-result))
                } else {
                    Ok(())
                };
            }
            if reason.is_none()
                && let Some(error) = cancellation(deadline, queue_failed)
            {
                reason = Some(error);
                self.push(
                    opcode::AsyncCancel::new(CONTROL)
                        .build()
                        .user_data(CANCEL)
                        .into(),
                );
            }
            if reason.is_some()
                && let Err(error) = submit(&self.ring)
            {
                // Keep ownership until the original CQE arrives even if a
                // cancel submission fails. No STOP or DEL can overtake it.
                afs_logging::warn!("overlaybd.control_cancel_submit"; "error" => error.to_string());
            }
            let mut fd = libc::pollfd {
                fd: self.ring.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: fd is one initialized pollfd and the ring stays open.
            if unsafe { libc::poll(&mut fd, 1, CHECK_INTERVAL_MS) } < 0
                && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted
            {
                thread::sleep(Duration::from_millis(CHECK_INTERVAL_MS as u64));
            }
        }
    }

    fn push(&mut self, entry: squeue::Entry128) {
        // SAFETY: this ring is private and holds at most a command and its
        // cancellation. The only command FD is owned by Control.
        unsafe { self.ring.submission().push(&entry) }
            .expect("private ublk control ring has room for two entries");
    }
}

fn submit(ring: &IoUring<squeue::Entry128>) -> io::Result<()> {
    loop {
        match ring.submit() {
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn cancellation(
    deadline: Instant,
    queue_failed: Option<&watch::Receiver<bool>>,
) -> Option<io::Error> {
    if queue_failed.is_some_and(|failed| *failed.borrow()) {
        Some(io::Error::other("ublk queue failed during device startup"))
    } else if Instant::now() >= deadline {
        Some(io::Error::new(
            io::ErrorKind::TimedOut,
            "ublk control deadline expired",
        ))
    } else {
        None
    }
}
