use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::schema::{
    application, device, device_profile, multicast_group, multicast_group_device, tenant,
};
use super::{
    application::Application, device::Device, device_profile::DeviceProfile, tenant::Tenant,
};
use super::{error::Error, get_async_db_conn};
use lrwn::EUI64;
use sha2::{Digest, Sha256};
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

static SECRET_KEY: &[u8; 32] = b"e2fc714cef004ad48c91d2c08b8c4f75";
// Custom function for ts-lora
pub async fn get_channel_index(dev_eui: EUI64) -> Result<usize, Error> {
    let mut db_conn = get_async_db_conn().await?;

    // Query to find the multicast group ID for the given dev_eui
    let id: Uuid = multicast_group_device::table
        .filter(multicast_group_device::dsl::dev_eui.eq(dev_eui))
        .select(multicast_group_device::dsl::multicast_group_id)
        .first::<Uuid>(&mut db_conn)
        .await
        .map_err(|e| {
            Error::from_diesel(e, "Error while retrieving multicast group ID".to_string())
        })?;

    let counter = multicast_group::table
        .filter(multicast_group::dsl::id.eq(id))
        .select(multicast_group::dsl::f_cnt)
        .first::<i64>(&mut db_conn)
        .await
        .map_err(|e| {
            Error::from_diesel(
                e,
                "Error while retrieving frame counter from Multicast Group".to_string(),
            )
        })?;

    let channel_index = generate_channel(counter, SECRET_KEY, id);

    Ok(channel_index)
}

/// Generates a unique channel for a given group UUID.
fn generate_channel(asn: i64, key: &[u8], offset_uuid: Uuid) -> usize {
    let mut hasher = Sha256::new();
    hasher.update(asn.to_le_bytes());
    hasher.update(key);
    let hash_bytes = hasher.finalize();

    // Extract the 3 most significant bits from the hash.
    let msb_3 = (hash_bytes[0] >> 5) as usize;

    // Calculate OFFSET_j' using the UUID bytes.
    let sum_uuid_bytes: u64 = offset_uuid.as_bytes().iter().map(|&b| b as u64).sum();
    let offset_j_prime = ((sum_uuid_bytes as i64 + asn) % 8) as usize;

    // Convert OFFSET_j' to binary.
    let b_0 = (offset_j_prime >> 2) & 0b1;
    let b_1 = (offset_j_prime >> 1) & 0b1;
    let b_2 = offset_j_prime & 0b1;

    // XOR msb_3 and binary OFFSET_j'.
    let x_0 = (msb_3 >> 2) & 0b1;
    let x_1 = (msb_3 >> 1) & 0b1;
    let x_2 = msb_3 & 0b1;

    // Calculate and return the channel.
    (x_0 ^ b_0) << 2 | (x_1 ^ b_1) << 1 | (x_2 ^ b_2)
}
