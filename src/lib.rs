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
    ) -> impl std::future::Future<Output = Result<Self, Box<dyn std::error::Error>>>
    where
        Self: Sized;

    fn configure(
        self: std::sync::Arc<Self>,
        service_conf: &mut actix_web::web::ServiceConfig,
        namespace: &str,
    );
}

#[macro_export]
macro_rules! modules {
    (
        $infra:expr;
        $(
            ($namespace:expr, $module:ty)
        ),+ $(,)?
    ) => {{
        async {
            let infra = $infra;

            let mut modules: Vec<(
                &'static str,
                std::sync::Arc<dyn $crate::Module>,
            )> = Vec::new();

            $(
                let module_infra = infra.clone();

                let module = <$module as $crate::Module>::new(
                    module_infra
                )
                .await?;

                modules.push((
                    $namespace,
                    std::sync::Arc::new(module),
                ));
            )+

            Ok::<_, Box<dyn std::error::Error>>(modules)
        }
        .await
    }};
}
