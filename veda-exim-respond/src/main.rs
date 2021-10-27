#[macro_use]
extern crate log;
extern crate serde_derive;
extern crate serde_json;
use actix_web::App;
use actix_web::{get, put, HttpResponse};
use actix_web::{middleware, web, HttpServer};
use futures::lock::Mutex;
use futures::select;
use futures::FutureExt;
use serde_json::json;
use serde_json::Value;
use std::io;
use std::io::ErrorKind;
use v_common::module::module::{init_log_with_params, Module};
use v_common::module::veda_backend::Backend;
use v_common::onto::individual::{Individual, RawObj};
use v_common::v_api::api_client::MStorageClient;
use v_exim::*;
use v_exim::{create_export_message, decode_message, encode_message, processing_imported_message};
use v_queue::consumer::Consumer;
use v_queue::record::ErrorQueue;

#[get("/export_delta/{remote_node_id}")]
async fn export_delta(web::Path(remote_node_id): web::Path<String>) -> io::Result<HttpResponse> {
    // this request changes from master
    // читаем элемент очереди, создаем обьект и отправляем на server
    let consumer_name = format!("r_{}", remote_node_id.replace(":", "_"));
    let mut queue_consumer = Consumer::new("./data/out", &consumer_name, "extract").expect("!!!!!!!!! FAIL QUEUE");

    if let Err(e) = queue_consumer.queue.get_info_of_part(queue_consumer.id, true) {
        error!("get_info_of_part {}: {}", queue_consumer.id, e.as_str());
    }

    let size = queue_consumer.get_batch_size();
    info!("part: {}, elements: {}", queue_consumer.id, size);

    // пробуем взять из очереди заголовок сообщения
    if queue_consumer.pop_header() {
        let mut raw = RawObj::new(vec![0; (queue_consumer.header.msg_length) as usize]);

        if let Err(e) = queue_consumer.pop_body(&mut raw.data) {
            if e != ErrorQueue::FailReadTailMessage {
                error!("get msg from queue: {}", e.as_str());
            }
        } else {
            let queue_element = &mut Individual::new_raw(raw);
            match create_export_message(queue_element, &remote_node_id) {
                Ok(mut out_obj) => {
                    if let Ok(msg) = encode_message(&mut out_obj) {
                        queue_consumer.commit_and_next();
                        return Ok(HttpResponse::Ok().json(msg));
                    } else {
                        error!("fail encode out message");
                    }
                }
                Err(e) => {
                    if e != ExImCode::Ok {
                        error!("fail create out message {:?}", e);
                    } else {
                        queue_consumer.commit_and_next();
                    }
                }
            }
        }
    }

    Ok(HttpResponse::Ok().json(json!({"msg": ""})))
}

#[put("/import_delta")]
async fn import_delta(msg: web::Json<Value>, mstorage: web::Data<Mutex<MStorageClient>>, ctx: web::Data<Context>) -> io::Result<HttpResponse> {
    let node_id = ctx.node_id.clone();
    let sys_ticket = ctx.sys_ticket.clone();

    if let Ok(mut recv_indv) = decode_message(&msg.0) {
        let mut ms = mstorage.lock().await;
        let res = processing_imported_message(&node_id, &mut recv_indv, &sys_ticket, &mut ms);
        return Ok(HttpResponse::Ok().json(res));
    }
    Ok(HttpResponse::Ok().finish())
}

struct Context {
    node_id: String,
    sys_ticket: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    init_log_with_params("EXIM RESPOND", None, true);
    info!("START EXIM RESPOND");
    let mut backend = Backend::default();

    let param_name = "exim_respond_port";
    let exim_respond_port = Module::get_property(param_name);
    if exim_respond_port.is_none() {
        return Err(std::io::Error::new(ErrorKind::NotFound, format!("not found param {} in properties file", param_name)));
    }

    let sys_ticket;
    if let Ok(t) = backend.get_sys_ticket_id() {
        sys_ticket = t;
    } else {
        return Err(std::io::Error::new(ErrorKind::NotFound, format!("fail get system ticket")));
    }

    let mut node_id = get_db_id(&mut backend);
    if node_id.is_none() {
        node_id = create_db_id(&mut backend);
    }

    if node_id.is_none() {
        return Err(std::io::Error::new(ErrorKind::NotFound, format!("fail create node_id")));
    }
    let node_id = node_id.unwrap();
    info!("my node_id={}", node_id);

    let mut server_future = HttpServer::new(move || {
        App::new()
            .wrap(middleware::Compress::default())
            .wrap(
                middleware::DefaultHeaders::new()
                    .header("Server", "nginx/1.19.6")
                    .header("X-XSS-Protection", "1; mode=block")
                    .header("X-Content-Type-Options", "nosniff")
                    .header("X-Frame-Options", "sameorigin")
                    .header("Cache-Control", "no-cache, no-store, must-revalidate, private"),
            )
            .data(Context {
                node_id: node_id.clone(),
                sys_ticket: sys_ticket.clone(),
            })
            .data(Mutex::new(MStorageClient::new(Module::get_property("main_module_url").unwrap_or_default())))
            .service(export_delta)
            .service(import_delta)
    })
    .bind(format!("0.0.0.0:{}", exim_respond_port.unwrap().parse::<u16>().unwrap_or(5588)))?
    .run()
    .fuse();

    select! {
        _r = server_future => println!("Server is stopped!"),
    };

    Ok(())
}
