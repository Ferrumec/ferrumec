use crate::permission::PermissionSet;
use crate::permission::Permissions;
use actix_web::dev::Service;
use actix_web::{App, HttpServer};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing_actix_web::{DefaultRootSpanBuilder, TracingLogger};

use crate::{CacheFactory, Infra, Module, Observability, record_request};

/// Initializes observability and runs an Actix Web server hosting `modules`.
///
/// Each `(namespace, module)` pair is registered with [`Module::configure`]
/// on every worker. Requests are traced with `tracing-actix-web` and recorded
/// through [`record_request`] (count and duration by method, route and
/// status).
///
/// This is an opinionated bootstrap: it reports telemetry under the service
/// name `"mains"`, binds to `127.0.0.1:8080`, and panics if observability
/// cannot be initialized. Telemetry is flushed after the server stops. For
/// other settings, build your own `HttpServer` and use [`Observability`] and
/// [`record_request`] directly.
///
/// Requires the `launch` feature (which enables `web`).
pub async fn launch(
    modules: Vec<(&'static str, Arc<dyn Module>)>,
    infra: impl Infra,
) -> std::io::Result<()> {
    // ---------------------------------------------------------
    // Observability
    // ---------------------------------------------------------

    let telemetry = Observability::init("mains", env!("CARGO_PKG_VERSION"))
        .expect("failed to initialize observability");

    tracing::info!(
        "Observability initialized: logs=VictoriaLogs, \
         metrics=VictoriaMetrics, traces=OTLP"
    );

    // ---------------------------------------------------------
    // HTTP server
    // ---------------------------------------------------------

    let modules = Arc::new(modules);

    let permission_set = PermissionSet::from_file("permissions.json").unwrap_or_else(|e| {
        tracing::error!("failed to load permissions.json: {e}");
        panic!("failed to load permissions.json: {e}");
    });

    let session_store = infra
        .clone()
        .cache_factory()
        .new_cache("permission_sessions", Duration::from_mins(30));

    let permission_middleware = Permissions::new(permission_set, session_store);

    let host = std::env::var("HOST").unwrap_or("0.0.0.0".to_string());
    let port = std::env::var("PORT")
        .unwrap_or("8080".to_string())
        .parse::<u16>()
        .expect("invalid value for PORT");

    let server = HttpServer::new({
        let modules = Arc::clone(&modules);

        move || {
            let app = App::new()
                .wrap(permission_middleware.clone())
                .wrap(TracingLogger::<DefaultRootSpanBuilder>::new())
                .wrap_fn(|req, srv| {
                    let start = Instant::now();
                    let method = req.method().to_string();

                    let future = srv.call(req);

                    async move {
                        let response = future.await?;
                        let duration = start.elapsed();

                        let route = response
                            .request()
                            .match_pattern()
                            .unwrap_or_else(|| "<unmatched>".to_owned());

                        record_request(&method, &route, response.status().as_u16(), duration);

                        Ok(response)
                    }
                });

            // API routes first, so they take priority over the static catch-all.
            let mut api = actix_web::web::scope("/api");
            for (namespace, module) in modules.iter() {
                api = api.configure(|cfg| {
                    module.clone().configure(cfg, namespace);
                });
            }
            api = api.configure(|cfg| infra.clone().configure(cfg, "health"));

            let mut app = app.service(api);

            // Static frontend last: it matches everything under "/".
            if let Ok(frontend) = std::env::var("FRONTEND") {
                app = app.default_service(
                    actix_files::Files::new("/", frontend).index_file("index.html"),
                );
            }

            app
        }
    })
    .bind((host, port))?
    .run();

    // ---------------------------------------------------------
    // Wait for Actix to stop.
    // ---------------------------------------------------------

    let result = server.await;

    // Flush/shutdown telemetry providers after the HTTP server
    // has stopped accepting requests.
    telemetry.shutdown();
    result
}
