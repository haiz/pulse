use axum::http::{HeaderMap, StatusCode};

/// Extract API key from Authorization header or query param.
pub fn extract_token(headers: &HeaderMap, query_token: Option<&str>) -> Result<String, StatusCode> {
    // Try Authorization: Bearer <token>
    if let Some(auth) = headers.get("authorization") {
        if let Ok(auth_str) = auth.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                return Ok(token.to_string());
            }
        }
    }

    // Try query parameter
    if let Some(token) = query_token {
        if !token.is_empty() {
            return Ok(token.to_string());
        }
    }

    // Anonymous mode — empty token (broker decides whether to accept)
    Ok(String::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_from_bearer_header() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer psk_live_abc".parse().unwrap());
        let token = extract_token(&headers, None).unwrap();
        assert_eq!(token, "psk_live_abc");
    }

    #[test]
    fn extract_from_query_param() {
        let headers = HeaderMap::new();
        let token = extract_token(&headers, Some("psk_live_xyz")).unwrap();
        assert_eq!(token, "psk_live_xyz");
    }

    #[test]
    fn bearer_takes_precedence() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer from_header".parse().unwrap());
        let token = extract_token(&headers, Some("from_query")).unwrap();
        assert_eq!(token, "from_header");
    }

    #[test]
    fn anonymous_when_no_token() {
        let headers = HeaderMap::new();
        let token = extract_token(&headers, None).unwrap();
        assert_eq!(token, "");
    }
}
