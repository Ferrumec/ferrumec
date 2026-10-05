use crate::Infra;

/// A self-contained piece of a service (its repositories, handlers and
/// routes), built from an [`Infra`] and mounted on an Actix Web app.
///
/// Modules are constructed with [`Module::new`], usually through the
/// [`modules!`](crate::modules) macro, and then registered on each Actix
/// worker through [`Module::configure`].
pub trait Module: Send + Sync + 'static {
    /// Builds the module from the shared infrastructure.
    ///
    /// Take what you need from `infra`: the database pool, a cache factory,
    /// the event stream. This is also the place to register event
    /// subscribers or start an [`Outbox`](crate::event::Outbox).
    fn new(
        infra: impl Infra,
    ) -> impl std::future::Future<Output = Result<Self, Box<dyn std::error::Error>>>
    where
        Self: Sized;

    /// Registers the module's routes and services.
    ///
    /// Called once per Actix worker. `namespace` is the name the module was
    /// registered under in [`modules!`](crate::modules); use it as the URL
    /// scope so modules don't collide.
    fn configure(
        self: std::sync::Arc<Self>,
        service_conf: &mut actix_web::web::ServiceConfig,
        namespace: &str,
    );
}

/// Builds several [`Module`]s from one [`Infra`] value.
///
/// The infra expression is evaluated once and cloned for each module, so it
/// must implement [`Clone`]. The macro evaluates to
/// `Result<Vec<(&'static str, Arc<dyn Module>)>, Box<dyn Error>>`, which is
/// the argument type of `launch::launch`. It must be used in an async
/// context and constructs modules in the order given, stopping at the first
/// error.
///
/// ```ignore
/// let modules = ferrumec::modules!(
///     LocalInfra::new().await?;
///     ("users", UsersModule),
///     ("orders", OrdersModule),
/// )?;
/// ```
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
