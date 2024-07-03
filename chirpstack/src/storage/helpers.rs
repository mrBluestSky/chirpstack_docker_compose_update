use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::schema::{application, device, device_profile, tenant, multicast_group, multicast_group_device};
use super::{
    application::Application, device::Device, device_profile::DeviceProfile, tenant::Tenant, multicast::MulticastGroup
};
use super::{error::Error, get_async_db_conn};
use lrwn::EUI64;
use uuid::Uuid;

pub async fn get_all_device_data(
    dev_eui: EUI64,
) -> Result<(Device, Application, Tenant, DeviceProfile), Error> {
    let res = device::table
        .inner_join(application::table)
        .inner_join(tenant::table.on(application::dsl::tenant_id.eq(tenant::dsl::id)))
        .inner_join(device_profile::table)
        .filter(device::dsl::dev_eui.eq(&dev_eui))
        .first::<(Device, Application, Tenant, DeviceProfile)>(&mut get_async_db_conn().await?)
        .await
        .map_err(|e| Error::from_diesel(e, dev_eui.to_string()))?;
    Ok(res)
}

// Custom function for ts-lora
pub async fn get_multicast_group_index(
    dev_eui: EUI64,
) -> Result<usize, Error> {
    let mut db_conn = get_async_db_conn().await?;

    // Query to find the multicast group ID for the given dev_eui
    let device_multicast_group_id = multicast_group_device::table
        .filter(multicast_group_device::dsl::dev_eui.eq(dev_eui))
        .select(multicast_group_device::dsl::multicast_group_id)
        .first::<Uuid>(&mut db_conn)
        .await
        .map_err(|e| Error::from_diesel(e, "Error while retrieving multicast group ID".to_string()))?;

    
    // Get all multicast groups to find the index
    let multicast_groups: Vec<MulticastGroup> = multicast_group::table
        .load::<MulticastGroup>(&mut db_conn)
        .await
        .map_err(|e| Error::from_diesel(e, "Error while retrieving multicast groups".to_string()))?;

    // Find the index of the group with the matching ID
    multicast_groups.iter().enumerate()
        .find_map(|(index, group)| if group.id == device_multicast_group_id {
            Some(index)
        } else {
            None
        })
        .ok_or_else(|| Error::NotFound(format!("Device's multicast group not found among retrieved groups (id: {})", device_multicast_group_id)))
}