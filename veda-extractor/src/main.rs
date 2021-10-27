/*
 * Извлекает из текущей очереди [individuals-flow] и производит проверку
 * следует ли выгружать в другую систему. Проверка производится методом
 * is_exportable, если ответ успешный, то происходит запись в очередь
 * ./data/out/extract
 */
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;

use crate::v8_script::{is_exportable, load_exim_filter_scripts};
use std::{env, fs, thread, time};
use v_exim::*;
use v_queue::consumer::*;
use v_queue::queue::*;
use v_queue::record::*;
use v_v8::common::ScriptInfoContext;
use v_v8::jsruntime::JsRuntime;
use v_v8::scripts_workplace::ScriptsWorkPlace;
use v_v8::v_common::ft_xapian::xapian_reader::XapianReader;
use v_v8::v_common::module::common::load_onto;
use v_v8::v_common::module::info::ModuleInfo;
use v_v8::v_common::module::module::{get_cmd, get_inner_binobj_as_individual, init_log, Module, PrepareError};
use v_v8::v_common::module::remote_indv_r_storage::inproc_storage_manager;
use v_v8::v_common::module::veda_backend::Backend;
use v_v8::v_common::onto::datatype::Lang;
use v_v8::v_common::onto::individual::Individual;
use v_v8::v_common::onto::individual2msgpack::to_msgpack;
use v_v8::v_common::onto::onto::Onto;
use v_v8::v_common::search::common::FTQuery;
use v_v8::v_common::storage::common::StorageMode;
use v_v8::v_common::v_api::api_client::IndvOp;
use v_v8::v_common::v_api::obj::ResultCode;

mod v8_script;

pub struct Context<'a> {
    sys_ticket: String,
    db_id: String,
    queue_out: Queue,
    workplace: ScriptsWorkPlace<'a, ScriptInfoContext>,
    xr: XapianReader,
    onto: Onto,
}

fn main() -> Result<(), i32> {
    init_log("EXIM-EXTRACTOR");
    thread::spawn(move || inproc_storage_manager());

    let mut js_runtime = JsRuntime::new();
    listen_queue(&mut js_runtime)
}

fn listen_queue<'a>(js_runtime: &'a mut JsRuntime) -> Result<(), i32> {
    let module_info = ModuleInfo::new("./data", "extract", true);
    if module_info.is_err() {
        error!("{:?}", module_info.err());
        return Err(-1);
    }

    let mut backend = Backend::create(StorageMode::ReadOnly, false);
    let mut module = Module::default();
    while !backend.mstorage_api.connect() {
        error!("main module not ready, sleep and repeat");
        thread::sleep(time::Duration::from_millis(1000));
    }
    let sys_ticket;
    if let Ok(t) = backend.get_sys_ticket_id() {
        sys_ticket = t;
    } else {
        error!("fail get sys_ticket");
        return Ok(());
    }

    let mut onto = Onto::default();

    info!("load onto start");
    load_onto(&mut backend.storage, &mut onto);
    info!("load onto end");

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

    let queue_out = Queue::new("./data/out", "extract", Mode::ReadWrite).expect("!!!!!!!!! FAIL QUEUE");
    let mut queue_consumer = Consumer::new("./data/queue", "extract", "individuals-flow").expect("!!!!!!!!! FAIL QUEUE");

    if let Some(xr) = XapianReader::new("russian", &mut backend.storage) {
        let mut ctx = Context {
            sys_ticket,
            queue_out,
            db_id: my_node_id,
            workplace: ScriptsWorkPlace::new(js_runtime.v8_isolate()),
            xr,
            onto,
        };

        ctx.workplace.load_ext_scripts(&ctx.sys_ticket);
        load_exim_filter_scripts(&mut ctx.workplace, &mut ctx.xr);

        let args: Vec<String> = env::args().collect();
        for el in args.iter() {
            if el.starts_with("--query") {
                if let Some(i) = el.find('=') {
                    let query = el.to_string().split_off(i + 1).replace("\'", "'");
                    if let Err(e) = export_from_query(&query, &mut backend, &mut ctx) {
                        error!("fail execute query [{}], err={:?}", query, e);
                    }
                }
            }
        }

        module.listen_queue(
            &mut queue_consumer,
            &mut ctx,
            &mut (before_batch as fn(&mut Backend, &mut Context<'a>, batch_size: u32) -> Option<u32>),
            &mut (prepare as fn(&mut Backend, &mut Context<'a>, &mut Individual, my_consumer: &Consumer) -> Result<bool, PrepareError>),
            &mut (after_batch as fn(&mut Backend, &mut Context<'a>, prepared_batch_size: u32) -> Result<bool, PrepareError>),
            &mut (heartbeat as fn(&mut Backend, &mut Context<'a>) -> Result<(), PrepareError>),
            &mut backend,
        );
    }
    Ok(())
}

fn heartbeat(_backend: &mut Backend, _ctx: &mut Context) -> Result<(), PrepareError> {
    Ok(())
}

fn before_batch(_backend: &mut Backend, _ctx: &mut Context, _size_batch: u32) -> Option<u32> {
    None
}

fn after_batch(_backend: &mut Backend, _ctx: &mut Context, _prepared_batch_size: u32) -> Result<bool, PrepareError> {
    Ok(false)
}

fn prepare(backend: &mut Backend, ctx: &mut Context, queue_element: &mut Individual, _my_consumer: &Consumer) -> Result<bool, PrepareError> {
    let cmd = get_cmd(queue_element);
    if cmd.is_none() {
        error!("cmd is none");
        return Ok(true);
    }

    let event_id = queue_element.get_first_literal("event_id").unwrap_or_default();
    if event_id.starts_with("exim") {
        return Ok(true);
    }

    let mut prev_state = Individual::default();
    get_inner_binobj_as_individual(queue_element, "prev_state", &mut prev_state);

    let mut new_state = Individual::default();
    get_inner_binobj_as_individual(queue_element, "new_state", &mut new_state);

    let user_id = queue_element.get_first_literal("user_uri").unwrap_or_default();
    let id = queue_element.get_first_literal("uri").unwrap_or_default();

    let date = queue_element.get_first_integer("date");
    //    if date.is_none() {
    //        return Ok(());
    //    }
    prepare_indv(backend, ctx, &id, cmd.unwrap(), Some(&mut prev_state), &mut new_state, &user_id, date.unwrap_or_default(), &queue_element.get_id())
}

fn prepare_indv(
    backend: &mut Backend,
    ctx: &mut Context,
    id: &str,
    cmd: IndvOp,
    prev_state: Option<&mut Individual>,
    new_state: &mut Individual,
    user_id: &str,
    date: i64,
    msg_id: &str,
) -> Result<bool, PrepareError> {
    let mut export_list = is_exportable(backend, ctx, prev_state, new_state, user_id);
    if export_list.is_empty() {
        return Ok(true);
    }

    for el in export_list.iter_mut() {
        if let Some(indv) = &mut el.indv {
            if indv.any_exists("rdf:type", &["v-s:File"]) {
                let src_full_path = "data/files".to_owned()
                    + &indv.get_first_literal("v-s:filePath").unwrap_or_default()
                    + "/"
                    + &indv.get_first_literal("v-s:fileUri").unwrap_or_default();

                if let Ok(f) = fs::read(src_full_path) {
                    indv.add_binary("v-s:fileData", f);
                }
            }
            let res = add_to_queue(id, &mut ctx.queue_out, cmd.clone(), indv, msg_id, &ctx.db_id, &el.target, date, el.enable_scripts);
            if let Err(e) = res {
                error!("fail prepare message, err={:?}", e);
                return Err(PrepareError::Fatal);
            }
        } else {
            let res = add_to_queue(id, &mut ctx.queue_out, cmd.clone(), new_state, msg_id, &ctx.db_id, &el.target, date, el.enable_scripts);
            if let Err(e) = res {
                error!("fail prepare message, err={:?}", e);
                return Err(PrepareError::Fatal);
            }
        }
    }

    Ok(true)
}

fn add_to_queue(
    id: &str,
    queue_out: &mut Queue,
    cmd: IndvOp,
    new_state_indv: &mut Individual,
    msg_id: &str,
    source: &str,
    target: &str,
    date: i64,
    enable_scripts: bool,
) -> Result<(), i32> {
    new_state_indv.parse_all();

    let mut raw: Vec<u8> = Vec::new();
    if to_msgpack(&new_state_indv, &mut raw).is_ok() {
        let mut new_indv = Individual::default();
        new_indv.set_id(msg_id);
        new_indv.add_uri("uri", id);
        if !new_state_indv.is_empty() {
            new_indv.add_binary("new_state", raw);
        }
        new_indv.add_integer("cmd", cmd.to_i64());
        new_indv.add_integer("date", date);
        new_indv.add_string("source_veda", source, Lang::NONE);
        new_indv.add_string("target_veda", target, Lang::NONE);
        new_indv.add_bool("enable_scripts", enable_scripts);

        info!("export: cmd={}, uri={}, src={}, target={}, enable_scripts={}", cmd.as_string(), id, &source, &target, enable_scripts);

        let mut raw1: Vec<u8> = Vec::new();
        if let Err(e) = to_msgpack(&new_indv, &mut raw1) {
            error!("fail serialize, err={:?}", e);
            return Err(-2);
        }
        if let Err(e) = queue_out.push(&raw1, MsgType::Object) {
            error!("fail push into queue, err={:?}", e);
            return Err(-1);
        }
    }
    Ok(())
}

fn export_from_query(query: &str, backend: &mut Backend, ctx: &mut Context) -> Result<(), PrepareError> {
    let mut ftq = FTQuery::new_with_user("cfg:VedaSystem", query);
    ftq.top = 100000;
    ftq.limit = 100000;
    info!("execute query [{:?}]", ftq);
    let res = ctx.xr.query(ftq, &mut backend.storage);
    if res.result_code == ResultCode::Ok && res.count > 0 {
        for id in &res.result {
            if let Some(mut indv) = backend.get_individual(id, &mut Individual::default()) {
                let msg_id = indv.get_id().to_string();
                prepare_indv(backend, ctx, &indv.get_id().to_string(), IndvOp::Put, None, &mut indv, "", 0, &msg_id)?;
            }
        }
    }
    Ok(())
}
