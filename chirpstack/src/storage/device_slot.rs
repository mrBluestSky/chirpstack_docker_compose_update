use anyhow::Result;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use tracing::info;
use uuid::Uuid;

use lrwn::{DevAddr, EUI64};

use super::error::Error;
use super::get_async_db_conn;
use super::schema::device_slot;

#[derive(Queryable, Insertable, AsChangeset, PartialEq, Eq, Debug, Clone)]
#[diesel(table_name = device_slot)]
pub struct DeviceSlot {
    pub dev_eui: EUI64,
    pub dev_addr: Option<DevAddr>,
    pub slot: Option<i32>,
    pub multicast_group_id: Uuid,
    pub created_at: DateTime<Utc>,
}

impl Default for DeviceSlot {
    fn default() -> Self {
        let now = Utc::now();

        DeviceSlot {
            dev_eui: EUI64::from_be_bytes([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]),
            dev_addr: None,
            slot: None,
            multicast_group_id: Uuid::new_v4(),
            created_at: now,
        }
    }
}

// Creates an record in the device_slot table with a given information about the record in the ds variable
pub async fn create(ds: DeviceSlot) -> Result<DeviceSlot, Error> {
    let ds: DeviceSlot = diesel::insert_into(device_slot::table)
        .values(&ds)
        .get_result(&mut get_async_db_conn().await?)
        .await
        .map_err(|e| Error::from_diesel(e, ds.dev_eui.to_string()))?;
    info!(
        dev_eui = %ds.dev_eui,
        "Device slot created"
    );
    Ok(ds)
}

// return a device's time slot for its dev_eui value
pub async fn get(dev_eui: &EUI64) -> Result<DeviceSlot, Error> {
    let ds = device_slot::dsl::device_slot
        .find(&dev_eui)
        .first(&mut get_async_db_conn().await?)
        .await
        .map_err(|e| Error::from_diesel(e, dev_eui.to_string()))?;
    Ok(ds)
}

// give a new device_slot value for a given end device
pub async fn update(ds: DeviceSlot) -> Result<DeviceSlot, Error> {
    let ds: DeviceSlot = diesel::update(device_slot::dsl::device_slot.find(&ds.dev_eui))
        .set(&ds)
        .get_result(&mut get_async_db_conn().await?)
        .await
        .map_err(|e| Error::from_diesel(e, ds.dev_eui.to_string()))?;
    info!(
        dev_eui = %ds.dev_eui,
        "Device slot updated"
    );
    Ok(ds)
}

// delete a device slot instance attached to an end device
pub async fn delete(dev_eui: &EUI64) -> Result<(), Error> {
    let ra = diesel::delete(device_slot::dsl::device_slot.find(&dev_eui))
        .execute(&mut get_async_db_conn().await?)
        .await?;
    if ra == 0 {
        return Err(Error::NotFound(dev_eui.to_string()));
    }
    info!(
        dev_eui = %dev_eui,
        "Device slot deleted"
    );
    Ok(())
}
