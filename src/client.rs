use std::io::Read;

use hyper::client::Client as HyperClient;
use hyper::client::RequestBuilder;
use hyper::error::Error;
use hyper::header::{Authorization, Basic};
use hyper::method::Method;
use hyper::status::StatusCode;
use url::Url;

use ::BatchPoints;

pub trait Client {
    fn ping(&self) -> bool;
    fn write(&self, bp: BatchPoints) -> bool;
    fn query(&self, q: &str, database: &str) -> String;
}

#[derive(Debug)]
pub struct HttpClient {
    credentials: Option<Basic>,
    url: Url,
    client: HyperClient,
}

impl HttpClient {
    pub fn new(url: &str) -> HttpClient {
        HttpClient {
            credentials: None,
            url: Url::parse(url).unwrap(),
            client: HyperClient::new(),
        }
    }

    pub fn credentials(mut self, username: &str, password: &str) -> HttpClient {
        self.credentials = Some(
            Basic {
                username: username.to_string(),
                password: Some(password.to_string())
            }
        );

        self
    }

    fn request(&self, method: Method, url: Url) -> RequestBuilder {
        let builder = self.client.request(method, url);

        match self.credentials.clone() {
            Some(credentials) => builder.header(Authorization(credentials)),
            None => builder,
        }
    }

    fn url(&self, input: &str) -> Url {
        self.url.join(input).unwrap()
    }

    fn url_with_params(&self, input: &str, params: &[(&str, &str)]) -> Url {
        Url::parse_with_params(self.url(input).as_str(), params).unwrap()
    }
}

impl Client for HttpClient {
    fn ping(&self) -> bool {
        self.request(Method::Get, self.url.join("ping").unwrap())
            .send()
            .and_then(|res| {
                match res.status {
                    StatusCode::NoContent => Ok(()),
                    _ => Err(Error::Status),
                }
            })
            .is_ok()
    }

    fn write(&self, bp: BatchPoints) -> bool {
        let body = bp.points
            .iter()
            .map(|p| p.serialize())
            .collect::<Vec<_>>()
            .join("\n");

        self.request(Method::Post, self.url_with_params("write", &[("db", bp.database)]))
            .body(body.as_str())
            .send()
            .and_then(|res| {
                match res.status {
                    StatusCode::NoContent => Ok(()),
                    _ => Err(Error::Status),
                }
            })
            .is_ok()
    }

    fn query(&self, q: &str, database: &str) -> String {
        let url = self
            .url_with_params("query", &[("db", database), ("q", q)]);

        let mut response = self
            .request(Method::Get, url)
            .send()
            .unwrap();

        let mut body = String::new();
        response.read_to_string(&mut body).unwrap();

        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use point::*;

    #[test]
    fn client() {
        let client = HttpClient::new("http://localhost:8086")
            .credentials("username", "passwd");
        assert!(client.ping());

        let point = Point::new("cpu_usage")
            .timestamp(1000)
            .tag("cpu", "cpu-total")
            .field("idle", Field::Float(89.3))
            .field("busy", Field::Float(10.7));

        assert!(client.write(BatchPoints::one("test", point)));
    }
}
