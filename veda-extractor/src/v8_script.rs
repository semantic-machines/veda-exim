use crate::Context;
use std::collections::HashSet;
use std::sync::Mutex;
use v_v8::callback::*;
use v_v8::common::*;
use v_v8::rusty_v8 as v8;
use v_v8::rusty_v8::{ContextScope, HandleScope, Local, Value};
use v_v8::scripts_workplace::ScriptsWorkPlace;
use v_v8::session_cache::CallbackSharedData;
use v_v8::v_common::ft_xapian::xapian_reader::XapianReader;
use v_v8::v_common::module::veda_backend::Backend;
use v_v8::v_common::onto::individual::Individual;
use v_v8::v_common::search::common::FTQuery;
use v_v8::v_common::v_api::obj::ResultCode;

lazy_static! {
    static ref INIT_LOCK: Mutex<u32> = Mutex::new(0);
}

#[must_use]
pub struct SetupGuard {}

impl Drop for SetupGuard {
    fn drop(&mut self) {
        // TODO shutdown process cleanly.
    }
}

#[derive(Debug)]
pub struct OutValue {
    pub target: String,
    pub indv: Option<Individual>,
    pub enable_scripts: bool,
}

pub fn is_exportable(
    backend: &mut Backend,
    ctx: &mut Context,
    prev_state_indv: Option<&mut Individual>,
    new_state_indv: &mut Individual,
    user_id: &str,
) -> Vec<OutValue> {
    let mut ov = vec![];

    new_state_indv.parse_all();

    let mut rdf_types = new_state_indv.get_literals("rdf:type").unwrap_or_default();

    let mut session_data = CallbackSharedData::default();

    if let Some(indv) = prev_state_indv {
        session_data.g_key2indv.insert("$prev_state".to_owned(), Individual::new_from_obj(indv.get_obj()));
    }
    session_data.g_key2indv.insert("$document".to_owned(), Individual::new_from_obj(new_state_indv.get_obj()));
    session_data.g_key2attr.insert("$ticket".to_owned(), ctx.sys_ticket.to_owned());
    if !user_id.is_empty() {
        session_data.g_key2attr.insert("$user".to_owned(), user_id.to_string());
    } else {
        session_data.g_key2attr.insert("$user".to_owned(), "cfg:VedaSystem".to_owned());
    }
    //session_data.set_g_super_classes(&rdf_types, &ctx.onto);

    let mut super_classes = HashSet::new();
    for indv_type in rdf_types.iter() {
        ctx.onto.get_supers(indv_type, &mut super_classes);
    }
    for el in super_classes {
        rdf_types.push(el);
    }
    rdf_types.push("rdfs:Resource".to_owned());

    let mut sh_g_vars = G_VARS.lock().unwrap();
    let g_vars = sh_g_vars.get_mut();
    *g_vars = session_data;
    drop(sh_g_vars);

    for script_id in ctx.workplace.scripts_order.iter() {
        if let Some(script) = ctx.workplace.scripts.get(script_id) {
            if let Some(compiled_script) = script.compiled_script {
                if !is_filter_pass(script, new_state_indv.get_id(), &rdf_types, &mut ctx.onto) {
                    debug!("skip (filter) script:{}", script_id);
                    continue;
                }

                debug!("script_id={}, doc_id={}", script_id, new_state_indv.get_id());

                let mut scope = ContextScope::new(&mut ctx.workplace.scope, ctx.workplace.context);
                if let Some(res) = compiled_script.run(&mut scope) {
                    if res.is_array() {
                        if let Some(res) = res.to_object(&mut scope) {
                            if let Some(key_list) = res.get_property_names(&mut scope) {
                                for resources_idx in 0..key_list.length() {
                                    let j_resources_idx = v8::Integer::new(&mut scope, resources_idx as i32);
                                    if let Some(v) = res.get(&mut scope, j_resources_idx.into()) {
                                        prepare_out_obj(backend, &mut ov, v, &mut scope);
                                    }
                                }
                            }
                        }
                    } else if res.is_object() {
                        prepare_out_obj(backend, &mut ov, res, &mut scope);
                    } else if res.is_string() {
                        if let Some(s) = res.to_string(scope.as_mut()) {
                            let target = s.to_rust_string_lossy(&mut scope);
                            if !target.is_empty() {
                                ov.push(OutValue {
                                    target,
                                    indv: None,
                                    enable_scripts: false,
                                });
                            }
                        }
                    } else if res.is_null_or_undefined() {
                        debug!("empty result");
                    } else {
                        error!("unknown result type");
                    }
                }
            }
        }
    }
    ov
}

fn prepare_out_obj(backend: &mut Backend, ov: &mut Vec<OutValue>, res: Local<Value>, scope: &mut ContextScope<HandleScope>) {
    if let Some(out_obj) = res.to_object(scope) {
        let to_key = str_2_v8(scope, "to");
        let indv_key = str_2_v8(scope, "indv");
        let indv_id_key = str_2_v8(scope, "indv_id");
        let enable_scripts_key = str_2_v8(scope, "enable_scripts");

        let mut target = String::new();

        if let Some(v_to) = out_obj.get(scope, to_key.into()) {
            if let Some(v) = v_to.to_string(scope) {
                target = v.to_rust_string_lossy(scope);
            }
        }

        let mut indv = None;

        if let Some(v_indv_id) = out_obj.get(scope, indv_id_key.into()) {
            if !v_indv_id.is_null_or_undefined() {
                if let Some(v) = v_indv_id.to_string(scope) {
                    let id = &v.to_rust_string_lossy(scope);
                    if let Some(mut i) = backend.get_individual_h(id) {
                        i.parse_all();
                        indv = Some(*i);
                    }
                }
            } else if let Some(v_indv) = out_obj.get(scope, indv_key.into()) {
                if !v_indv.is_null_or_undefined() {
                    if let Some(o) = v_indv.to_object(scope) {
                        let mut ri = Individual::default();
                        v8obj_into_individual(scope, o, &mut ri);
                        indv = Some(ri);
                    }
                }
            }
        }

        let mut enable_scripts = false;

        if let Some(v_enable_scripts) = out_obj.get(scope, enable_scripts_key.into()) {
            if v_enable_scripts.is_boolean() {
                let b = v_enable_scripts.to_boolean(scope);
                enable_scripts = b.to_integer(scope).unwrap().value() != 0;
            }
        }

        ov.push(OutValue {
            target,
            indv,
            enable_scripts,
        });
    }
}

pub(crate) fn load_exim_filter_scripts(wp: &mut ScriptsWorkPlace<ScriptInfoContext>, xr: &mut XapianReader) {
    let res = xr.query(FTQuery::new_with_user("cfg:VedaSystem", "'rdf:type' === 'v-s:EximFilter'"), &mut wp.backend.storage);

    if res.result_code == ResultCode::Ok && res.count > 0 {
        for id in &res.result {
            if let Some(ev_indv) = wp.backend.get_individual(id, &mut Individual::default()) {
                prepare_script(wp, ev_indv);
            }
        }
    }
    info!("load scripts from db: {:?}", wp.scripts_order);
}

pub(crate) fn prepare_script(wp: &mut ScriptsWorkPlace<ScriptInfoContext>, ev_indv: &mut Individual) {
    if ev_indv.is_exists_bool("v-s:deleted", true) || ev_indv.is_exists_bool("v-s:disabled", true) {
        info!("disable script {}", ev_indv.get_id());
        if let Some(scr_inf) = wp.scripts.get_mut(ev_indv.get_id()) {
            scr_inf.compiled_script = None;
        }
        return;
    }

    if let Some(script_text) = ev_indv.get_first_literal("v-s:script") {
        let str_script = "\
      (function () { \
        try { \
          var ticket = get_env_str_var ('$ticket'); \
          var document = get_individual (ticket, '$document'); \
          var prev_state = get_individual (ticket, '$prev_state'); \
          var super_classes = get_env_str_var ('$super_classes'); \
          var user_uri = get_env_str_var ('$user'); \
          "
        .to_owned()
            + &script_text
            + " \
         } catch (e) { log_trace (e); } \
      })();";

        let mut scr_inf: ScriptInfo<ScriptInfoContext> = ScriptInfo::new_with_src(&ev_indv.get_id(), &str_script);

        scr_inf.context.prevent_by_type = HashVec::new(ev_indv.get_literals("v-s:preventByType").unwrap_or_default());
        scr_inf.context.trigger_by_uid = HashVec::new(ev_indv.get_literals("v-s:triggerByUid").unwrap_or_default());
        scr_inf.context.trigger_by_type = HashVec::new(ev_indv.get_literals("v-s:triggerByType").unwrap_or_default());
        scr_inf.dependency = HashVec::new(ev_indv.get_literals("v-s:dependency").unwrap_or_default());

        wp.add_to_order(&scr_inf);

        let scope = &mut v8::ContextScope::new(&mut wp.scope, wp.context);
        scr_inf.compile_script(ev_indv.get_id(), scope);
        wp.scripts.insert(scr_inf.id.to_string(), scr_inf);
    } else {
        error!("v-s:script no found");
    }
}
