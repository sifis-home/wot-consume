use std::{sync::OnceLock, time::Duration};

use reqwest::{Client, StatusCode};
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};
use wot_consume::{
    consume, HandleJsonResponseError, NumericSubtypeError, PropertyReaderSendFutureError,
    PropertyReaderSendFutureInvalidResponse,
};
use wot_td::{
    builder::{
        BuildableInteractionAffordance, IntegerDataSchemaBuilderLike, SpecializableDataSchema,
    },
    protocol::http,
    thing::Minimum,
    Thing,
};

fn client() -> Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();

    CLIENT
        .get_or_init(|| {
            Client::builder()
                .timeout(Duration::from_secs(1))
                .connect_timeout(Duration::from_secs(1))
                .build()
                .unwrap()
        })
        .clone()
}

async fn check_no_uri_variables_used(endpoints: &[&str], mock_server: &MockServer) {
    mock_server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| endpoints.contains(&request.url.path()))
        .for_each(|request| assert!(request.url.query().is_none()));
}

#[tokio::test]
async fn get_simple_property() {
    let mock_server = MockServer::start().await;
    let td = Thing::builder("test")
        .ext(http::HttpProtocol {})
        .finish_extend()
        .base(mock_server.uri())
        .property("test", |b| {
            b.ext(())
                .ext_interaction(())
                .ext_data_schema(())
                .finish_extend_data_schema()
                .integer()
                .form(|b| b.ext(http::Form::default()).href("/testing-url"))
        })
        .build()
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/testing-url"))
        .respond_with(ResponseTemplate::new(200).set_body_json(42))
        .expect(1)
        .mount(&mock_server)
        .await;

    let prop: i16 = consume(&td, client())
        .await
        .unwrap()
        .property("test")
        .unwrap()
        .read()
        .send()
        .unwrap()
        .await
        .unwrap();

    assert_eq!(prop, 42);
    mock_server.verify().await;
    check_no_uri_variables_used(&["/testing-url"], &mock_server).await;
}

#[tokio::test]
async fn missing_property_in_td() {
    let mock_server = MockServer::start().await;
    let td = Thing::builder("test")
        .ext(http::HttpProtocol {})
        .finish_extend()
        .base(mock_server.uri())
        .property("test", |b| {
            b.ext(())
                .ext_interaction(())
                .ext_data_schema(())
                .finish_extend_data_schema()
                .integer()
                .form(|b| b.ext(http::Form::default()).href("/testing-url"))
        })
        .build()
        .unwrap();

    assert!(consume(&td, client())
        .await
        .unwrap()
        .property("missing-test")
        .is_none());

    mock_server.verify().await;
}

#[tokio::test]
async fn missing_property_from_server() {
    let mock_server = MockServer::start().await;
    let td = Thing::builder("test")
        .ext(http::HttpProtocol {})
        .finish_extend()
        .base(mock_server.uri())
        .property("test", |b| {
            b.ext(())
                .ext_interaction(())
                .ext_data_schema(())
                .finish_extend_data_schema()
                .integer()
                .form(|b| b.ext(http::Form::default()).href("/testing-url"))
        })
        .build()
        .unwrap();

    let err = consume(&td, client())
        .await
        .unwrap()
        .property("test")
        .unwrap()
        .read::<i16>()
        .send()
        .unwrap()
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        PropertyReaderSendFutureError::ResponseError(reqwest_error)
        if reqwest_error.is_status() &&
        reqwest_error.status() == Some(StatusCode::NOT_FOUND)
    ));
    mock_server.verify().await;
}

#[tokio::test]
async fn failing_minimum_integer() {
    let mock_server = MockServer::start().await;
    let td = Thing::builder("test")
        .ext(http::HttpProtocol {})
        .finish_extend()
        .base(mock_server.uri())
        .property("test", |b| {
            b.ext(())
                .ext_interaction(())
                .ext_data_schema(())
                .finish_extend_data_schema()
                .integer()
                .minimum(43)
                .form(|b| b.ext(http::Form::default()).href("/testing-url"))
        })
        .build()
        .unwrap();

    Mock::given(method("GET"))
        .and(path("/testing-url"))
        .respond_with(ResponseTemplate::new(200).set_body_json(42))
        .expect(1)
        .mount(&mock_server)
        .await;

    let err = consume(&td, client())
        .await
        .unwrap()
        .property("test")
        .unwrap()
        .read::<i16>()
        .send()
        .unwrap()
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        PropertyReaderSendFutureError::InvalidResponse(
            PropertyReaderSendFutureInvalidResponse::Json(HandleJsonResponseError::IntegerSubtype(
                NumericSubtypeError::Minimum {
                    expected: Minimum::Inclusive(43),
                    found: 42,
                }
            ),),
        ),
    ));
    mock_server.verify().await;
    check_no_uri_variables_used(&["/testing-url"], &mock_server).await;
}
