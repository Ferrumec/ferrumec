pub mod cache;
pub mod event;
pub mod infra;

pub use cache::{Store,CacheFactory};
pub use event::{Event, EventStream, EventType, Subscriber};
pub use infra::{Infra, LocalInfra};

pub trait Module: Sized {
    fn new(
        infras: impl Infra,
    ) -> impl std::future::Future<Output = Result<Self, Box<dyn std::error::Error>>>;
    fn configure<T>(self: std::sync::Arc<Self>, service_conf: T);
}
