// This file has been modified to support ts-lora
use anyhow::Result;
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use lrwn::{DevAddr, EUI64};
use rand::seq::SliceRandom;
use rand::RngCore;
use sha2::{Digest, Sha256};
// use std::convert::TryInto;
// use tracing::info;
use num_bigint::BigUint;
use uuid::Uuid;

use crate::config;
use crate::storage::device_slot;
use crate::storage::device_slot::DeviceSlot;
use crate::storage::schema::{
    application, device, device_slot as schema_device_slot, multicast_group_device, tenant,
};
use crate::storage::{get_async_db_conn, AsyncPgPoolConnection};

const NUMBER_OF_SLOTS: u32 = 64;
static mut CURRENT_SLOT: u32 = 0;

pub async fn get_random_dev_addr_slot(dev_eui: EUI64) -> Result<DevAddr> {
    let mut conn = get_async_db_conn().await?;

    // Fetch multicast group ID and max slot count
    let multicast_group_id: Uuid = multicast_group_device::table
        .filter(multicast_group_device::dsl::dev_eui.eq(dev_eui))
        .select(multicast_group_device::dsl::multicast_group_id)
        .first(&mut conn)
        .await?;
    let max_slot_count = device::table
        .inner_join(application::table.on(application::dsl::id.eq(device::dsl::application_id)))
        .inner_join(tenant::table.on(tenant::dsl::id.eq(application::dsl::tenant_id)))
        .select(tenant::dsl::max_slot_count)
        .filter(device::dsl::dev_eui.eq(dev_eui))
        .first::<i32>(&mut conn)
        .await?;

    // Get current device slot record, if exists
    let existing_ds = device_slot::get(&dev_eui).await;
    let (new_slot, dev_addr): (i32, DevAddr);

    // Determine if new slot needs to be calculated or reused
    if let Ok(existing_ds) = existing_ds {
        if existing_ds.multicast_group_id == multicast_group_id {
            // Reuse existing slot and regenerate device address
            new_slot = existing_ds.slot.expect("Existing slot must be present");
            dev_addr = regenerate_dev_addr_for_slot(new_slot, max_slot_count);
        } else {
            // Find new slot and generate device address
            (new_slot, dev_addr) =
                calculate_new_slot_and_dev_addr(&mut conn, multicast_group_id, max_slot_count)
                    .await?;
        }
        // Update existing record with new slot or multicast group
        let new_ds = DeviceSlot {
            dev_eui,
            dev_addr: Some(dev_addr),
            slot: Some(new_slot),
            multicast_group_id,
            created_at: Utc::now(),
        };
        device_slot::update(new_ds).await?;
    } else {
        // No existing record, find new slot and generate device address
        (new_slot, dev_addr) =
            calculate_new_slot_and_dev_addr(&mut conn, multicast_group_id, max_slot_count).await?;
        // Create new record
        let new_ds = DeviceSlot {
            dev_eui,
            dev_addr: Some(dev_addr),
            slot: Some(new_slot),
            multicast_group_id,
            created_at: Utc::now(),
        };
        device_slot::create(new_ds).await?;
    }

    Ok(dev_addr)
}

async fn calculate_new_slot_and_dev_addr(
    conn: &mut AsyncPgPoolConnection,
    multicast_group_id: Uuid,
    max_slot_count: i32,
) -> Result<(i32, DevAddr)> {
    let used_slots: Vec<i32> = schema_device_slot::table
        .filter(schema_device_slot::dsl::multicast_group_id.eq(multicast_group_id))
        .select(schema_device_slot::dsl::slot)
        .load::<Option<i32>>(conn)
        .await?
        .into_iter()
        .filter_map(|slot| slot)
        .collect();

    let mut new_slot = 0;
    for i in 0..max_slot_count {
        if !used_slots.contains(&i) {
            new_slot = i;
            break;
        }
    }
    // Fallback: use the next available slot after the last one, if all are taken
    if new_slot == 0 && used_slots.len() == max_slot_count as usize {
        new_slot = ((*used_slots.iter().max().unwrap_or(&0) + 1) as i32) % max_slot_count;
    }

    let dev_addr = regenerate_dev_addr_for_slot(new_slot, max_slot_count);
    Ok((new_slot, dev_addr))
}

fn regenerate_dev_addr_for_slot(slot: i32, max_slot_count: i32) -> DevAddr {
    // Get new random DevAddr
    let mut dev_addr: DevAddr = generate_dev_addr();

    // Keep regenerating DevAddr until satisfies formula
    // slot = (int(crypto_hash(DevAddr))) % max_slot_count
    loop {
        let mut hasher = Sha256::new();
        hasher.update(&dev_addr.to_be_bytes());
        let sha256_hash = hasher.finalize();

        // let big_int = BigUint::from_bytes_be(&sha256_hash);
        // let hash_int = &big_int % (max_slot_count as u32);

        // // If slot matches, break the loop
        // if hash_int == BigUint::from(slot as u32) {
        //     break;
        // }
        let mut big_int = 0u64;
        for &byte in sha256_hash.iter().take(8) {
            big_int = (big_int << 8) | byte as u64;
        }
        
        let hash_int = (big_int % max_slot_count as u64) as i32;

        if hash_int % max_slot_count == slot {
            break;
        }

        dev_addr = generate_dev_addr();
    }
    dev_addr
}

// Function to generate random bytes for DevAddr
fn generate_dev_addr() -> DevAddr {
    let conf = config::get();
    let mut rng = rand::thread_rng();

    // Get configured DevAddr prefixes.
    let prefixes = if conf.network.dev_addr_prefixes.is_empty() {
        vec![conf.network.net_id.dev_addr_prefix()]
    } else {
        conf.network.dev_addr_prefixes.clone()
    };

    // Pick a random one (in case multiple prefixes are configured).
    let prefix = *prefixes.choose(&mut rng).unwrap();

    // Generate a random DevAddr
    let mut dev_addr_bytes: [u8; 4] = [0; 4];
    rng.fill_bytes(&mut dev_addr_bytes);

    let mut dev_addr = DevAddr::from_be_bytes(dev_addr_bytes);
    dev_addr.set_dev_addr_prefix(prefix);

    dev_addr
}

// Old implementation
// project/chirpstack/src/devaddr.rs
pub fn get_random_dev_addr() -> DevAddr {
    // check whether we still have any time slots left
    unsafe {
        // old implementation, panic if I run out of time slots
        /*
        if CURRENT_SLOT == NUMBER_OF_SLOTS {
            panic!("no free time slots left!");
        }*/

        // new implementation, counts slots from zero
        if CURRENT_SLOT == NUMBER_OF_SLOTS {
            CURRENT_SLOT = 0;
        }
    }

    let conf = config::get();
    let mut rng = rand::thread_rng();

    // Get configured DevAddr prefixes.
    let prefixes = if conf.network.dev_addr_prefixes.is_empty() {
        vec![conf.network.net_id.dev_addr_prefix()]
    } else {
        conf.network.dev_addr_prefixes.clone()
    };

    // Pick a random one (in case multiple prefixes are configured).
    let prefix = *prefixes.choose(&mut rng).unwrap();

    // Generate random DevAddr.
    let mut dev_addr: [u8; 4] = [0; 4];
    rng.fill_bytes(&mut dev_addr);
    #[cfg(test)]
    {
        dev_addr = [1, 2, 3, 4];
    }
    let mut dev_addr = DevAddr::from_be_bytes(dev_addr);

    // Set DevAddr prefix.
    dev_addr.set_dev_addr_prefix(prefix);

    // print DevAddr as 4 integers
    //dbg!(dev_addr.clone());
    // print the address prefix (AddrPrefix)
    //dbg!(prefix);

    // find the slot of the DevAddr

    // hash function
    let sum: u32 = dev_addr.clone().into_iter().map(|x| x as u32).sum();
    let generated_slot: u32 = sum % NUMBER_OF_SLOTS;

    // regenerate the dev_addr until the correct one is generated.
    loop {
        unsafe {
            match CURRENT_SLOT == generated_slot {
                true => {
                    // define the next time slot to generate
                    CURRENT_SLOT = CURRENT_SLOT + 1;
                    // print the generated time slot and devaddr value
                    println!(
                        "{:?} connected, time slot is: {}",
                        dev_addr.clone(),
                        generated_slot
                    );
                    break;
                }
                false => {
                    return get_random_dev_addr();
                }
            };
        }
    }
    dev_addr
}
