use actix_web::dev::Service;
use actix_web::{App, HttpServer};
use std::sync::Arc;
use std::time::Instant;

use tracing_actix_web::{DefaultRootSpanBuilder, TracingLogger};

use crate::{Module, Observability, record_request};

pub async fn launch(modules: Vec<(&'static str, Arc<dyn Module>)>) -> std::io::Result<()> {
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

    let server = HttpServer::new({
        let modules = Arc::clone(&modules);

        move || {
            let mut app = App::new()
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

            for (namespace, module) in modules.iter() {
                app = app.configure(|cfg| {
                    module.clone().configure(cfg, namespace);
                });
            }

            app
        }
    })
    .bind(("127.0.0.1", 8080))?
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
