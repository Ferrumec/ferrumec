use actix_web::HttpMessage;
use actix_web::body::{BoxBody, MessageBody};
use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform, forward_ready};
use actix_web::{Error, HttpResponse};
use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use crate::Store;
use uuid::Uuid;

use super::permission::PermissionSet;

#[derive(Clone)]
pub struct Permissions {
    permission_set: Arc<PermissionSet>,
    store: Arc<dyn Store<Uuid, Authority>>,
}

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct Authority {
    pub user: Uuid,
    pub namespace: Uuid,
    pub role: u128,
}

impl Permissions {
    pub fn new(permission_set: PermissionSet, store: Arc<dyn Store<Uuid, Authority>>) -> Self {
        Self {
            permission_set: Arc::new(permission_set),
            store,
        }
    }
}

impl<S, B> Transform<S, ServiceRequest> for Permissions
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<BoxBody>;
    type Error = Error;
    type InitError = ();
    type Transform = PermissionsMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(PermissionsMiddleware {
            service: Rc::new(service),
            store: Arc::clone(&self.store),
            permission_set: Arc::clone(&self.permission_set),
        }))
    }
}

pub struct PermissionsMiddleware<S> {
    service: Rc<S>,
    permission_set: Arc<PermissionSet>,
    store: Arc<dyn Store<Uuid, Authority>>,
}

impl<S, B> Service<ServiceRequest> for PermissionsMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<BoxBody>;
    type Error = Error;

    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let method = req.method().clone();
        let path = req.path().to_string();

        let permission = self.permission_set.find(&method, &path);

        // No permission configured: default allow.
        let Some(perm) = permission else {
            let service = Rc::clone(&self.service);

            return Box::pin(async move {
                let res = service.call(req).await?;
                Ok(res.map_into_boxed_body())
            });
        };

        // IMPORTANT:
        // `perm` is borrowed from `self.permission_set`.
        // Copy the value we need so the async future doesn't borrow `self`.
        let bit_id = perm.bit_id;

        let Some(cookie) = req.cookie("permission") else {
            return Box::pin(async move {
                Ok(req.into_response(HttpResponse::Unauthorized().finish()))
            });
        };

        let Ok(id) = Uuid::parse_str(cookie.value()) else {
            return Box::pin(async move {
                Ok(req.into_response(HttpResponse::InternalServerError().finish()))
            });
        };

        let store = Arc::clone(&self.store);
        let service = Rc::clone(&self.service);

        Box::pin(async move {
            let authority = match store.get(&id).await {
                Ok(Some(authority)) => authority,

                Ok(None) => {
                    return Ok(req.into_response(HttpResponse::Forbidden().finish()));
                }

                Err(_) => {
                    return Ok(req.into_response(HttpResponse::InternalServerError().finish()));
                }
            };

            req.extensions_mut().insert(authority.clone());

            let role = authority.role;

            let bit_mask = 1u128 << bit_id;

            if role & bit_mask == 0 {
                return Ok(req.into_response(HttpResponse::Forbidden().finish()));
            }

            let res = service.call(req).await?;

            Ok(res.map_into_boxed_body())
        })
    }
}
