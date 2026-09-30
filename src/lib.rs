pub mod cache;
pub mod event;
pub mod infra;
pub mod launch;
pub mod observability;
pub use cache::{CacheFactory, Store};
pub use event::{Event, EventStream, EventType, Subscriber};
pub use infra::{Infra, LocalInfra};
pub use observability::{Observability, record_request};
pub trait Module: Send + Sync + 'static {
    fn new(
        infra: impl Infra,
    ) -> impl std::future::Future<
        Output = Result<Self, Box<dyn std::error::Error>>
    >
    where
        Self: Sized;

    fn configure(
        self: std::sync::Arc<Self>,
        service_conf: &mut actix_web::web::ServiceConfig,
        namespace: &str,
    );
}