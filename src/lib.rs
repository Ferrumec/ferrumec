pub mod cache;
pub mod event;
pub mod infra;

pub trait Module: Sized {
    fn new(
        infras: impl infra::Infra,
    ) -> impl std::future::Future<Output = Result<Self, Box<dyn std::error::Error>>>;
    fn configure<T>(self: std::sync::Arc<Self>, service_conf: T);
}
