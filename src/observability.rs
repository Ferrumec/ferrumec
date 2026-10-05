use std::{error::Error, sync::OnceLock, time::Duration};

use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Histogram, MeterProvider},
    trace::TracerProvider,
};

use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::WithExportConfig;

use opentelemetry_sdk::{
    Resource, logs::SdkLoggerProvider, metrics::SdkMeterProvider, trace::SdkTracerProvider,
};

use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

static REQUESTS: OnceLock<Counter<u64>> = OnceLock::new();
static REQUEST_DURATION: OnceLock<Histogram<f64>> = OnceLock::new();

/// Handles to the OpenTelemetry providers installed by [`Observability::init`].
///
/// Logs, metrics and traces are exported over OTLP/HTTP. Endpoints can be
/// overridden with environment variables:
///
/// | Variable                | Default                                                  |
/// | ----------------------- | -------------------------------------------------------- |
/// | `OTEL_LOGS_ENDPOINT`    | `http://127.0.0.1:9428/insert/opentelemetry/v1/logs`     |
/// | `OTEL_METRICS_ENDPOINT` | `http://127.0.0.1:8428/opentelemetry/v1/metrics`         |
/// | `OTEL_TRACES_ENDPOINT`  | `http://127.0.0.1:10428/insert/opentelemetry/v1/traces`  |
///
/// The defaults match a local VictoriaLogs / VictoriaMetrics / VictoriaTraces
/// setup. Call [`Observability::shutdown`] before exit to flush pending data.
pub struct Observability {
    logs: SdkLoggerProvider,
    metrics: SdkMeterProvider,
    traces: SdkTracerProvider,
}

impl Observability {
    /// Installs the global meter, tracer and `tracing` subscriber.
    ///
    /// The subscriber combines an `EnvFilter` (from `RUST_LOG`, defaulting to
    /// `info`), an OpenTelemetry log bridge, an OpenTelemetry trace layer and
    /// a console `fmt` layer. `service_name` and `version` become the
    /// `service.name` and `service.version` resource attributes.
    ///
    /// Fails if an exporter cannot be built, or if this was already called in
    /// the process (the request metrics and the global subscriber can only be
    /// set once).
    pub fn init(service_name: &str, version: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let resource = Resource::builder()
            .with_service_name(service_name.to_owned())
            .with_attribute(KeyValue::new("service.version", version.to_owned()))
            .build();

        // ---------------------------------------------------------
        // Logs -> VictoriaLogs
        // ---------------------------------------------------------

        let log_exporter = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_endpoint(std::env::var("OTEL_LOGS_ENDPOINT").unwrap_or_else(|_| {
                "http://127.0.0.1:9428/insert/opentelemetry/v1/logs".to_owned()
            }))
            .build()?;

        let logs = SdkLoggerProvider::builder()
            .with_batch_exporter(log_exporter)
            .with_resource(resource.clone())
            .build();

        // ---------------------------------------------------------
        // Metrics -> VictoriaMetrics
        // ---------------------------------------------------------

        let metric_exporter =
            opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .with_endpoint(std::env::var("OTEL_METRICS_ENDPOINT").unwrap_or_else(|_| {
                    "http://127.0.0.1:8428/opentelemetry/v1/metrics".to_owned()
                }))
                .build()?;

        let metrics = SdkMeterProvider::builder()
            .with_periodic_exporter(metric_exporter)
            .with_resource(resource.clone())
            .build();

        global::set_meter_provider(metrics.clone());

        // OpenTelemetry 0.33's MeterProvider::meter() requires
        // &'static str. Since service_name is runtime data,
        // construct an InstrumentationScope instead.
        let scope = InstrumentationScope::builder(service_name.to_owned()).build();

        let meter = metrics.meter_with_scope(scope);

        REQUESTS
            .set(
                meter
                    .u64_counter("http.server.request.count")
                    .with_description("Number of completed HTTP requests")
                    .build(),
            )
            .map_err(|_| "HTTP request counter already initialized")?;

        REQUEST_DURATION
            .set(
                meter
                    .f64_histogram("http.server.request.duration")
                    .with_unit("s")
                    .with_description("HTTP request duration in seconds")
                    .build(),
            )
            .map_err(|_| "HTTP request histogram already initialized")?;

        // ---------------------------------------------------------
        // Traces -> OTLP-compatible trace backend
        // ---------------------------------------------------------

        let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(std::env::var("OTEL_TRACES_ENDPOINT").unwrap_or_else(|_| {
                "http://127.0.0.1:10428/insert/opentelemetry/v1/traces".to_owned()
            }))
            .with_timeout(Duration::from_secs(2))
            .build()?;

        let traces = SdkTracerProvider::builder()
            .with_batch_exporter(trace_exporter)
            .with_resource(resource)
            .build();

        global::set_tracer_provider(traces.clone());

        // ---------------------------------------------------------
        // tracing subscriber
        // ---------------------------------------------------------

        let scope = InstrumentationScope::builder(service_name.to_owned()).build();

        let tracer = traces.tracer_with_scope(scope);
        let log_layer = OpenTelemetryTracingBridge::new(&logs);

        tracing_subscriber::registry()
            .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
            .with(log_layer)
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .with(tracing_subscriber::fmt::layer())
            .try_init()?;

        Ok(Self {
            logs,
            metrics,
            traces,
        })
    }

    /// Flushes and shuts down the log, trace and metric providers.
    ///
    /// Every provider is shut down even if an earlier one fails; failures are
    /// printed to stderr.
    pub fn shutdown(&self) {
        // Attempt every shutdown even if one provider fails.

        if let Err(error) = self.logs.shutdown() {
            eprintln!("Log provider shutdown failed: {error}");
        }

        if let Err(error) = self.traces.shutdown() {
            eprintln!("Trace provider shutdown failed: {error}");
        }

        if let Err(error) = self.metrics.shutdown() {
            eprintln!("Metric provider shutdown failed: {error}");
        }
    }
}

/// Record metrics after an HTTP request has completed.
pub fn record_request(method: &str, route: &str, status: u16, duration: Duration) {
    let attributes = [
        KeyValue::new("http.request.method", method.to_owned()),
        KeyValue::new("http.route", route.to_owned()),
        KeyValue::new("http.response.status_code", status as i64),
    ];

    if let Some(counter) = REQUESTS.get() {
        counter.add(1, &attributes);
    }

    if let Some(histogram) = REQUEST_DURATION.get() {
        histogram.record(duration.as_secs_f64(), &attributes);
    }
}
