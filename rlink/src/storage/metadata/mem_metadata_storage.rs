use std::sync::Mutex;

use crate::runtime::{ClusterDescriptor, HeartbeatItem, TaskManagerStatus};
use crate::storage::metadata::TMetadataStorage;
use crate::utils;

lazy_static! {
    pub static ref METADATA_STORAGE: Mutex<Option<ClusterDescriptor>> = Mutex::new(None);
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MemoryMetadataStorage {}

impl MemoryMetadataStorage {
    pub fn new() -> Self {
        MemoryMetadataStorage {}
    }
}

impl TMetadataStorage for MemoryMetadataStorage {
    fn save(&mut self, metadata: ClusterDescriptor) -> anyhow::Result<()> {
        let mut lock = METADATA_STORAGE
            .lock()
            .expect("METADATA_STORAGE lock failed");
        *lock = Some(metadata);

        debug!("Save metadata {:?}", lock.clone().unwrap());
        Ok(())
    }

    fn delete(&mut self) -> anyhow::Result<()> {
        let mut lock = METADATA_STORAGE
            .lock()
            .expect("METADATA_STORAGE lock failed");
        *lock = None;

        debug!("Delete metadata {:?}", lock.clone().unwrap());
        Ok(())
    }

    fn load(&self) -> anyhow::Result<ClusterDescriptor> {
        let lock = METADATA_STORAGE
            .lock()
            .expect("METADATA_STORAGE lock failed");
        Ok(lock.clone().unwrap())
    }

    fn update_application_status(
        &self,
        job_manager_status: TaskManagerStatus,
    ) -> anyhow::Result<()> {
        let mut lock = METADATA_STORAGE
            .lock()
            .expect("METADATA_STORAGE lock failed");
        let mut job_descriptor: ClusterDescriptor = lock.clone().unwrap();
        job_descriptor.coordinator_manager.coordinator_status = job_manager_status;

        *lock = Some(job_descriptor);
        Ok(())
    }

    fn update_task_manager_status(
        &self,
        task_manager_id: String,
        heartbeat_items: Vec<HeartbeatItem>,
        task_manager_status: TaskManagerStatus,
    ) -> anyhow::Result<()> {
        let mut update_success = false;

        let mut lock = METADATA_STORAGE
            .lock()
            .expect("METADATA_STORAGE lock failed");
        let mut job_descriptor: ClusterDescriptor = lock.clone().unwrap();
        for mut task_manager_descriptor in &mut job_descriptor.worker_managers {
            if task_manager_descriptor
                .task_manager_id
                .eq(task_manager_id.as_str())
            {
                task_manager_descriptor.task_status = task_manager_status;
                task_manager_descriptor.latest_heart_beat_ts =
                    utils::date_time::current_timestamp_millis();

                for heartbeat_item in heartbeat_items {
                    match heartbeat_item {
                        HeartbeatItem::MetricsAddress(addr) => {
                            task_manager_descriptor.metrics_address = addr;
                        }
                        HeartbeatItem::WorkerManagerAddress(addr) => {
                            task_manager_descriptor.task_manager_address = addr;
                        }
                        HeartbeatItem::WorkerManagerWebAddress(addr) => {
                            task_manager_descriptor.web_address = addr;
                        }
                        HeartbeatItem::HeartBeatStatus(status) => {
                            task_manager_descriptor.latest_heart_beat_status = status;
                        }
                        HeartbeatItem::TaskThreadId { task_id, thread_id } => {
                            for task_descriptor in &mut task_manager_descriptor.task_descriptors {
                                if task_descriptor.task_id.eq(&task_id) {
                                    task_descriptor.thread_id = format!("0x{:x}", thread_id);
                                }
                            }
                        }
                    }
                }

                update_success = true;
                break;
            }
        }

        if update_success {
            debug!("Update TaskManager metadata success. {:?}", job_descriptor);
            *lock = Some(job_descriptor);
            Ok(())
        } else {
            error!(
                "TaskManager(task_manager_id={}) metadata not found",
                task_manager_id
            );
            Err(anyhow!("metadata not found"))
        }
    }
}
