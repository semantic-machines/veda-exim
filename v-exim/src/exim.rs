#[macro_use]
extern crate enum_primitive_derive;
#[macro_use]
extern crate log;
extern crate base64;

pub mod configuration;
use crate::configuration::Configuration;

use base64::{decode, encode};
use http::StatusCode;
use num_traits::{FromPrimitive, ToPrimitive};
use serde_json::json;
use serde_json::value::Value as JSONValue;
use std::collections::HashMap;
use std::error::Error;
use std::fs::*;
use std::io::ErrorKind;
use std::io::Write;
use std::{thread, time};
use uuid::*;
use v_common::module::veda_backend::Backend;
use v_common::onto::datatype::Lang;
use v_common::onto::individual::{Individual, RawObj};
use v_common::onto::individual2msgpack::to_msgpack;
use v_common::onto::parser::parse_raw;
use v_common::v_api::api_client::{IndvOp, MStorageClient, ALL_MODULES};
use v_common::v_api::obj::ResultCode;
use v_queue::consumer::*;
use v_queue::record::*;

const TRANSMIT_FAILED: i64 = 32;

#[derive(Primitive, PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
#[repr(i64)]
pub enum ExImCode {
    Unknown = 0,
    Ok = 1,
    InvalidMessage = 2,
    InvalidCmd = 4,
    InvalidTarget = 8,
    FailUpdate = 16,
    TransmitFailed = TRANSMIT_FAILED,
    SendFailed = 64 | TRANSMIT_FAILED,
    ReceiveFailed = 128 | TRANSMIT_FAILED,
}

impl From<i64> for ExImCode {
    fn from(value: i64) -> Self {
        if let Some(v) = ExImCode::from_i64(value) {
            v
        } else {
            ExImCode::Unknown
        }
    }
}

impl From<ExImCode> for i64 {
    fn from(value: ExImCode) -> Self {
        if let Some(v) = value.to_i64() {
            v
        } else {
            0
        }
    }
}

impl ExImCode {
    pub fn as_string(&self) -> String {
        match self {
            ExImCode::Ok => "ok",
            ExImCode::InvalidMessage => "invalid message",
            ExImCode::InvalidCmd => "invalid cmd",
            ExImCode::InvalidTarget => "invalid target",
            ExImCode::FailUpdate => "fail update",
            ExImCode::TransmitFailed => "fail transmit",
            ExImCode::SendFailed => "fail send",
            ExImCode::ReceiveFailed => "fail receive",
            // ...
            ExImCode::Unknown => "unknown",
        }
        .to_string()
    }
}

pub fn send_changes_to_node(queue_consumer: &mut Consumer, resp_api: &Configuration, node_id: &str) -> (i32, ExImCode) {
    let mut size_batch = 0;
    let mut count_sent = 0;

    // read queue current part info
    if let Err(e) = queue_consumer.queue.get_info_of_part(queue_consumer.id, true) {
        error!("get_info_of_part {}: {}", queue_consumer.id, e.as_str());
        return (count_sent, ExImCode::InvalidMessage);
    }

    let delta = queue_consumer.queue.count_pushed - queue_consumer.count_popped;
    if delta == 0 {
        // if not new messages, read queue info
        queue_consumer.queue.get_info_queue();

        if queue_consumer.queue.id > queue_consumer.id {
            size_batch = 1;
        }
    } else if delta > 0 {
        if queue_consumer.queue.id != queue_consumer.id {
            size_batch = 1;
        } else {
            size_batch = queue_consumer.queue.count_pushed - queue_consumer.count_popped;
        }
    }

    // prepare packet and send to slave node
    if size_batch > 0 {
        info!("queue: batch size={}", size_batch);

        for (total_prepared_count, _it) in (0..size_batch).enumerate() {
            // пробуем взять из очереди заголовок сообщения
            if !queue_consumer.pop_header() {
                break;
            }

            let mut raw = RawObj::new(vec![0; (queue_consumer.header.msg_length) as usize]);

            // заголовок взят успешно, занесем содержимое сообщения в структуру Individual
            if let Err(e) = queue_consumer.pop_body(&mut raw.data) {
                if e != ErrorQueue::FailReadTailMessage {
                    error!("{} get msg from queue: {}", total_prepared_count, e.as_str());
                }
                break;
            }

            let queue_element = &mut Individual::new_raw(raw);

            let mut res = ExImCode::SendFailed;
            for attempt_count in 0..10 {
                let msg = create_export_message(queue_element, node_id);

                match msg {
                    Ok(mut msg) => {
                        if let Err(e) = send_export_message(&mut msg, resp_api) {
                            error!("fail send export message, err={:?}, attempt_count={}", e, attempt_count);

                            if attempt_count >= 9 {
                                res = ExImCode::SendFailed;
                                break;
                            }
                        } else {
                            count_sent += 1;
                            res = ExImCode::Ok;
                            break;
                        }
                    },
                    Err(e) => {
                        if e == ExImCode::Ok {
                            res = e;
                            break;
                        }
                        error!("fail create export message, err={:?}", e);
                        res = ExImCode::InvalidMessage;
                        break;
                    },
                }

                thread::sleep(time::Duration::from_millis(attempt_count * 100));
            }

            if res == ExImCode::Ok {
                queue_consumer.commit();

                if total_prepared_count % 1000 == 0 {
                    info!("get from queue, count: {}", total_prepared_count);
                }
            } else {
                return (count_sent, res);
            }
        }
    }
    (count_sent, ExImCode::Ok)
}

pub fn create_export_message(queue_element: &mut Individual, node_id: &str) -> Result<Individual, ExImCode> {
    if parse_raw(queue_element).is_ok() {
        let target_veda = queue_element.get_first_literal("target_veda");
        if target_veda.is_none() {
            return Err(ExImCode::InvalidMessage);
        }

        let target_veda = target_veda.unwrap_or_default();
        if target_veda != "*" && target_veda != node_id {
            return Err(ExImCode::Ok);
        }

        let wcmd = queue_element.get_first_integer("cmd");
        if wcmd.is_none() {
            return Err(ExImCode::InvalidMessage);
        }
        let cmd = IndvOp::from_i64(wcmd.unwrap_or_default());

        let new_state = queue_element.get_first_binobj("new_state");
        if cmd != IndvOp::Remove && new_state.is_none() {
            return Err(ExImCode::InvalidMessage);
        }

        let source_veda = queue_element.get_first_literal("source_veda");
        if source_veda.is_none() {
            return Err(ExImCode::InvalidMessage);
        }

        let date = queue_element.get_first_integer("date");
        if date.is_none() {
            return Err(ExImCode::InvalidMessage);
        }

        let wid = queue_element.get_first_literal("uri");
        if wid.is_none() {
            return Err(ExImCode::InvalidMessage);
        }
        let id = wid.unwrap_or_default();

        let enable_scripts = queue_element.get_first_bool("enable_scripts").unwrap_or(false);

        let mut indv = Individual::new_raw(RawObj::new(new_state.unwrap_or_default()));
        if parse_raw(&mut indv).is_ok() {
            indv.parse_all();

            let mut raw: Vec<u8> = Vec::new();
            if to_msgpack(&indv, &mut raw).is_ok() {
                let mut new_msg = Individual::default();
                new_msg.set_id(&format!("{}_{}", cmd.as_string(), &id));
                new_msg.add_uri("uri", &id);
                new_msg.add_binary("new_state", raw);
                new_msg.add_integer("cmd", cmd.to_i64());
                new_msg.add_integer("date", date.unwrap_or_default());
                new_msg.add_string("source_veda", &source_veda.unwrap_or_default(), Lang::none());
                new_msg.add_string("target_veda", &target_veda, Lang::none());
                new_msg.add_bool("enable_scripts", enable_scripts);

                return Ok(new_msg);
            }
            // info! ("{:?}", raw);
        }
    }
    Err(ExImCode::InvalidMessage)
}

pub fn encode_message(out_obj: &mut Individual) -> Result<JSONValue, Box<dyn Error>> {
    out_obj.parse_all();

    let mut raw1: Vec<u8> = Vec::new();
    to_msgpack(out_obj, &mut raw1)?;
    let msg_base64 = encode(raw1.as_slice());

    Ok(json!({ "msg": &msg_base64 }))
}

pub fn decode_message(src: &JSONValue) -> Result<Individual, Box<dyn Error>> {
    if let Some(props) = src.as_object() {
        if let Some(msg) = props.get("msg") {
            if let Some(m) = msg.as_str() {
                if m.is_empty() {
                    return Ok(Individual::default());
                }
                let mut recv_indv = Individual::new_raw(RawObj::new(decode(m)?));
                if parse_raw(&mut recv_indv).is_ok() {
                    return Ok(recv_indv);
                }
            }
        }
    }

    Err(Box::new(std::io::Error::new(ErrorKind::Other, "fail decode import message".to_owned())))
}

fn send_export_message(out_obj: &mut Individual, resp_api: &Configuration) -> Result<IOResult, Box<dyn Error>> {
    let uri_str = format!("{}/import_delta", resp_api.base_path);

    let res = resp_api.client.put(&uri_str).json(&encode_message(out_obj)?).send()?;

    if res.status() != StatusCode::OK {
        error!("responce status ={}", res.status());
    }

    let jj: IOResult = res.json()?;
    info!("sucess send export message: res={:?}, id={}", jj.res_code, out_obj.get_id());

    Ok(jj)
}

pub fn recv_import_message(importer_id: &str, resp_api: &Configuration) -> Result<JSONValue, Box<dyn Error>> {
    let uri_str = format!("{}/export_delta/{}", resp_api.base_path, importer_id);
    let msg: JSONValue = resp_api.client.get(&uri_str).send()?.json()?;
    Ok(msg)
}

#[macro_use]
extern crate serde_derive;

#[derive(Serialize, Deserialize)]
pub struct IOResult {
    pub id: String,
    pub res_code: ExImCode,
}

impl IOResult {
    pub fn new(id: &str, res_code: ExImCode) -> Self {
        IOResult {
            id: id.to_owned(),
            res_code,
        }
    }
}

pub fn processing_imported_message(my_node_id: &str, recv_msg: &mut Individual, systicket: &str, mstorage_client: &mut MStorageClient) -> IOResult {
    let wcmd = recv_msg.get_first_integer("cmd");
    if wcmd.is_none() {
        return IOResult::new(recv_msg.get_id(), ExImCode::InvalidCmd);
    }
    let cmd = IndvOp::from_i64(wcmd.unwrap_or_default());

    let source_veda = recv_msg.get_first_literal("source_veda");
    if source_veda.is_none() {
        return IOResult::new(recv_msg.get_id(), ExImCode::InvalidTarget);
    }

    let source_veda = source_veda.unwrap_or_default();
    if source_veda.len() < 32 {
        return IOResult::new(recv_msg.get_id(), ExImCode::InvalidTarget);
    }

    let target_veda = recv_msg.get_first_literal("target_veda");
    if target_veda.is_none() {
        return IOResult::new(recv_msg.get_id(), ExImCode::InvalidTarget);
    }
    let target_veda = target_veda.unwrap();

    if target_veda != "*" && my_node_id != target_veda {
        return IOResult::new(recv_msg.get_id(), ExImCode::InvalidTarget);
    }

    let enable_scripts = recv_msg.get_first_bool("enable_scripts").unwrap_or(false);

    let new_state = recv_msg.get_first_binobj("new_state");

    let mut indv = Individual::new_raw(RawObj::new(new_state.unwrap_or_default()));
    if parse_raw(&mut indv).is_ok() {
        indv.parse_all();

        if indv.any_exists("sys:source", &[my_node_id]) {
            indv.remove("sys:source");
        } else {
            indv.add_uri("sys:source", &source_veda);
        }

        if cmd == IndvOp::Remove {
            if let Some(id) = recv_msg.get_first_literal("uri") {
                indv.set_id(&id);
            }
        }

        if indv.any_exists("rdf:type", &["v-s:File"]) {
            if let Some(file_data) = indv.get_first_binobj("v-s:fileData") {
                let src_dir_path = "data/files".to_owned() + &indv.get_first_literal("v-s:filePath").unwrap_or_default();

                if let Err(e) = create_dir_all(src_dir_path.clone()) {
                    error!("fail create path: {:?}", e);
                }

                let src_full_path = src_dir_path + "/" + &indv.get_first_literal("v-s:fileUri").unwrap_or_default();

                match File::create(src_full_path.clone()) {
                    Ok(mut ofile) => {
                        if let Err(e) = ofile.write_all(&file_data) {
                            error!("fail write file: {:?}", e);
                        } else {
                            info!("success create file {}", src_full_path);
                        }
                    },
                    Err(e) => {
                        error!("fail create file: {:?}", e);
                    },
                }
                indv.remove("v-s:fileData");
            }
        }

        let src = if enable_scripts {
            "?"
        } else {
            "exim"
        };

        match mstorage_client.update_use_param(systicket, "exim", "?", ALL_MODULES, cmd, &indv) {
            Ok(_) => {
                info!("get from {}, success update, src={}, uri={}", source_veda, src, recv_msg.get_id());
                return IOResult::new(recv_msg.get_id(), ExImCode::Ok);
            },
            Err(e) => {
                error!("fail update, uri={}, result_code={:?}", recv_msg.get_id(), e.result);
                return IOResult::new(recv_msg.get_id(), ExImCode::FailUpdate);
            },
        }
    }

    IOResult::new(recv_msg.get_id(), ExImCode::FailUpdate)
}

pub fn load_linked_nodes(backend: &mut Backend, node_upd_counter: &mut i64, link_node_addresses: &mut HashMap<String, String>) {
    let mut node = Individual::default();

    if backend.storage.get_individual("cfg:standart_node", &mut node) {
        if let Some(c) = node.get_first_integer("v-s:updateCounter") {
            if c > *node_upd_counter {
                link_node_addresses.clear();
                if let Some(v) = node.get_literals("cfg:linked_node") {
                    for el in v {
                        let mut link_node = Individual::default();

                        if backend.storage.get_individual(&el, &mut link_node) && !link_node.is_exists("v-s:delete") {
                            if let Some(addr) = link_node.get_first_literal("rdf:value") {
                                link_node_addresses.insert(link_node.get_first_literal("cfg:node_id").unwrap_or_default(), addr);
                            }
                        }
                    }
                    info!("linked nodes: {:?}", link_node_addresses);
                }
                *node_upd_counter = c;
            }
        }
    }
}

pub fn get_db_id(backend: &mut Backend) -> Option<String> {
    let mut indv = Individual::default();
    if backend.storage.get_individual("cfg:system", &mut indv) {
        if let Some(c) = indv.get_first_literal("sys:id") {
            return Some(c);
        }
    }
    None
}

pub fn create_db_id(backend: &mut Backend) -> Option<String> {
    let systicket;
    if let Ok(t) = backend.get_sys_ticket_id() {
        systicket = t;
    } else {
        error!("fail get systicket");
        return None;
    }

    let uuid1 = "sys:".to_owned() + &Uuid::new_v4().to_hyphenated().to_string();
    info!("create new db id = {}", uuid1);

    let mut new_indv = Individual::default();
    new_indv.set_id(&uuid1);
    new_indv.add_uri("rdf:type", "sys:Node");
    if backend.mstorage_api.update(&systicket, IndvOp::Put, &new_indv).result != ResultCode::Ok {
        error!("fail create, uri={}", new_indv.get_id());
        return None;
    }

    new_indv = Individual::default();
    new_indv.set_id("cfg:system");
    new_indv.add_string("sys:id", &uuid1, Lang::none());
    let res = backend.mstorage_api.update(&systicket, IndvOp::Put, &new_indv);
    if res.result != ResultCode::Ok {
        error!("fail update, uri={}, result_code={:?}", new_indv.get_id(), res.result);
    } else {
        return Some(uuid1);
    }

    None
}
