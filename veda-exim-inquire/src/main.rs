/*
 * Инициирует и проводит сеансы обмена данными с внешними системами, является
 * master частью в протоколе связи. Знает о всех точках обмена
 * (load_linked_nodes)
 */
#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::{thread, time};
use v_common::module::module::init_log;
use v_common::module::veda_backend::Backend;
use v_common::storage::storage::StorageMode;
use v_exim::configuration::Configuration;
use v_exim::*;
use v_queue::consumer::*;

fn main() -> std::io::Result<()> {
    init_log("EXIM_INQUIRE");

    let mut backend = Backend::create(StorageMode::ReadOnly, false);

    let sys_ticket;
    if let Ok(t) = backend.get_sys_ticket_id() {
        sys_ticket = t;
    } else {
        error!("fail get system ticket");
        return Ok(());
    }

    // загрузка адресов связанных нод
    let mut node_upd_counter = 0;
    let mut link_node_addresses = HashMap::new();

    let mut my_node_id = get_db_id(&mut backend);
    if my_node_id.is_none() {
        my_node_id = create_db_id(&mut backend);

        if my_node_id.is_none() {
            error!("fail create Database Identification");
            return Ok(());
        }
    }
    let my_node_id = my_node_id.unwrap();
    info!("my node_id={}", my_node_id);

    load_linked_nodes(&mut backend, &mut node_upd_counter, &mut link_node_addresses);

    let mut sleep_time = 1000;

    loop {
        for (remote_node_id, remote_node_addr) in &link_node_addresses {
            let consumer_name = format!("i_{}", remote_node_id.replace(":", "_"));
            if let Ok(mut queue_consumer) = Consumer::new("./data/out", &consumer_name, "extract") {
                let exim_resp_api = Configuration::new(remote_node_addr, "", "");

                info!("send changes to node {}", consumer_name);
                let (count_sent, _res) = send_changes_to_node(&mut queue_consumer, &exim_resp_api, remote_node_id);

                if count_sent > 0 {
                    sleep_time = 1000;
                }

                // request changes from slave node
                info!("request changes form node {}", consumer_name);
                while let Ok(recv_msg) = recv_import_message(&my_node_id, &exim_resp_api) {
                    if let Ok(mut recv_indv) = decode_message(&recv_msg) {
                        let res = processing_imported_message(&my_node_id, &mut recv_indv, &sys_ticket, &mut backend.api);
                        if res.res_code != ExImCode::Ok {
                            error!("fail accept changes, uri={}, err={:?}", res.id, res.res_code);
                        } else {
                            sleep_time = 1000;
                            info!("get {} form node {}", recv_indv.get_id(), consumer_name);
                        }
                    }
                }
            }
        }
        thread::sleep(time::Duration::from_millis(sleep_time));

        if sleep_time < 30000 {
            sleep_time += 1000;
        }
    }
}
