use axum::extract::Request;
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::Router;
use std::sync::Arc;

/// Wraps `router` with bearer-token auth. devkitd runs privileged scripts,
/// so any bind outside loopback must carry a token — see the startup guard
/// in main.rs. Comparison is constant-time to avoid leaking the token via
/// response timing.
pub fn require_bearer_token(router: Router, token: Arc<str>) -> Router {
    router.layer(middleware::from_fn(move |req: Request, next: Next| {
        let token = Arc::clone(&token);
        async move {
            let authorized = req
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .map(|presented| ct_eq(presented, &token))
                .unwrap_or(false);

            if authorized {
                next.run(req).await
            } else {
                StatusCode::UNAUTHORIZED.into_response()
            }
        }
    }))
}

/// Constant-time string comparison (length + XOR-accumulate over bytes) so
/// an attacker probing the endpoint can't learn the token via early-exit
/// timing on a `==` comparison.
fn ct_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.bytes().zip(b.bytes()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt;

    fn app(token: &str) -> Router {
        let router = Router::new().route("/protected", get(|| async { "ok" }));
        require_bearer_token(router, Arc::from(token))
    }

    #[tokio::test]
    async fn rejects_missing_header() {
        let response = app("secret")
            .oneshot(HttpRequest::builder().uri("/protected").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejects_wrong_token() {
        let response = app("secret")
            .oneshot(
                HttpRequest::builder()
                    .uri("/protected")
                    .header(header::AUTHORIZATION, "Bearer nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn accepts_correct_token() {
        let response = app("secret")
            .oneshot(
                HttpRequest::builder()
                    .uri("/protected")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn ct_eq_matches_std_eq_semantics() {
        assert!(ct_eq("abc", "abc"));
        assert!(!ct_eq("abc", "abd"));
        assert!(!ct_eq("abc", "ab"));
    }
}
