use rocket::fairing::{Fairing, Info, Kind};
use rocket::http::{ContentType, Header, Method};
use rocket::{Request, Response};
use std::io::Cursor;

pub struct CORS();

impl Fairing for CORS {
    fn info(&self) -> Info {
        Info {
            name: "Add CORS headers to requests",
            kind: Kind::Response,
        }
    }

    fn on_response(&self, request: &Request, response: &mut Response) {
        if request.method() == Method::Options || response.content_type() == Some(ContentType::JSON)
        {
            response.set_header(Header::new("Access-Control-Allow-Origin", "*"));
            response.set_header(Header::new(
                "Access-Control-Allow-Methods",
                "POST, GET, OPTIONS, PATCH",
            ));
            response.set_header(Header::new(
                "Access-Control-Allow-Headers",
                "Content-Type, Authentication",
            ));
            response.set_header(Header::new("Access-Control-Allow-Credentials", "true"));
        }

        if request.method() == Method::Options {
            response.set_header(ContentType::Plain);
            response.set_sized_body(Cursor::new(""));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocket::local::Client;
    use rocket_contrib::json::JsonValue;

    #[options("/")]
    fn options_route() -> &'static str {
        "Hello, world!"
    }

    #[get("/")]
    fn get_json_route() -> JsonValue {
        json!("Hello, world!")
    }

    fn get_rocket_client() -> Client {
        let rocket = rocket::ignite()
            .mount("/", routes![options_route, get_json_route])
            .attach(CORS());
        let client = Client::new(rocket).expect("valid rocket instance");
        client
    }

    #[test]
    fn test_method_options() {
        let client = get_rocket_client();
        let req = client.options("/");
        let mut response = req.dispatch();
        let headers = response.headers();

        assert_eq!(headers.get_one("Access-Control-Allow-Origin"), Some("*"));
        assert_eq!(
            headers.get_one("Access-Control-Allow-Methods"),
            Some("POST, GET, OPTIONS, PATCH")
        );
        assert_eq!(
            headers.get_one("Access-Control-Allow-Credentials"),
            Some("true")
        );
        assert_eq!(
            headers.get_one("Content-Type"),
            Some("text/plain; charset=utf-8")
        );
        assert_eq!(response.body_string(), Some("".to_string()));
    }

    #[test]
    fn test_method_json() {
        let client = get_rocket_client();
        let req = client.get("/");
        let response = req.dispatch();
        let headers = response.headers();

        assert_eq!(headers.get_one("Access-Control-Allow-Origin"), Some("*"));
        assert_eq!(
            headers.get_one("Access-Control-Allow-Methods"),
            Some("POST, GET, OPTIONS, PATCH")
        );
        assert_eq!(
            headers.get_one("Access-Control-Allow-Credentials"),
            Some("true")
        );
    }
}
