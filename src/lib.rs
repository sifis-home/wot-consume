//! Web of Things consumer
//!
//! Provides all the building blocks to consume (use) [Web Of Things](https://www.w3.org/WoT/) Things.

mod data_schema_validator;
mod json;
mod sealed;

use std::{
    convert::identity,
    error::Error as StdError,
    fmt::{self, Display},
    future::Future,
    marker::PhantomData,
    num::NonZeroU64,
    ops::{BitOr, Not},
    rc::Rc,
    slice,
    str::FromStr,
};

use bitflags::bitflags;
use json::ValueRef;
use regex::Regex;
use reqwest::Url;
use sealed::HasHttpProtocolExtension;
use serde::de::DeserializeOwned;
use smallvec::{smallvec, SmallVec};
use tracing::warn;
use wot_td::{
    extend::ExtendableThing,
    protocol::http,
    thing::{
        self, ArraySchema, BoxedElemOrVec, DataSchema, DataSchemaSubtype, DefaultedFormOperations,
        FormOperation, IntegerSchema, InteractionAffordance, Maximum, Minimum, NumberSchema,
        ObjectSchema, PropertyAffordance, StringSchema,
    },
    Thing,
};
use zeroize::{Zeroize, ZeroizeOnDrop};

pub async fn get_thing_description(
    client: &reqwest::Client,
    host: &str,
    port: Option<u16>,
) -> Result<Thing, GetThingDescriptionError> {
    macro_rules! err {
        ($kind:expr) => {
            GetThingDescriptionError {
                host: host.to_owned(),
                kind: $kind,
            }
        };
    }

    let mut url: Url = host
        .parse()
        .map_err(|source| err!(GetThingDescriptionErrorKind::InvalidHost(source)))?;

    if url.path() != "/" {
        return Err(err!(GetThingDescriptionErrorKind::UrlHasPath));
    }

    match port {
        None | Some(443) => url
            .set_scheme("https")
            .map_err(|()| err!(GetThingDescriptionErrorKind::CannotSetScheme("https")))?,
        Some(port) => {
            url.set_scheme("http")
                .map_err(|()| err!(GetThingDescriptionErrorKind::CannotSetScheme("http")))?;
            url.set_port(Some(port))
                .map_err(|()| err!(GetThingDescriptionErrorKind::CannotSetPort(port)))?;
        }
    }

    url.set_path(".well-known/wot");

    client
        .get(url)
        .send()
        .await
        .map_err(|source| err!(GetThingDescriptionErrorKind::Request(source)))?
        .error_for_status()
        .map_err(|source| err!(GetThingDescriptionErrorKind::RequestStatus(source)))?
        .json()
        .await
        .map_err(|source| err!(GetThingDescriptionErrorKind::InvalidThing(source)))
}

#[derive(Debug)]
pub struct GetThingDescriptionError {
    pub host: String,
    pub kind: GetThingDescriptionErrorKind,
}

#[derive(Debug)]
pub enum GetThingDescriptionErrorKind {
    UrlHasPath,
    InvalidHost(url::ParseError),
    CannotSetScheme(&'static str),
    CannotSetPort(u16),
    Request(reqwest::Error),
    RequestStatus(reqwest::Error),
    InvalidThing(reqwest::Error),
}

impl Display for GetThingDescriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn write_init<'a, 'b>(
            err: &GetThingDescriptionError,
            f: &'a mut fmt::Formatter<'b>,
        ) -> Result<&'a mut fmt::Formatter<'b>, fmt::Error> {
            write!(
                f,
                r#"unable to get thing description from host "{}", "#,
                err.host
            )?;

            Ok(f)
        }

        match &self.kind {
            GetThingDescriptionErrorKind::UrlHasPath => write!(
                f,
                r#"unable to get thing description, "{}" is an url with a non-empty path"#,
                self.host
            ),
            GetThingDescriptionErrorKind::InvalidHost(_) => write!(
                f,
                r#"unable to get thing description, "{}" is not a valid host"#,
                self.host
            ),
            GetThingDescriptionErrorKind::CannotSetScheme(scheme) => write!(
                write_init(self, f)?,
                r#"unable to set scheme to "{scheme}""#
            ),
            GetThingDescriptionErrorKind::CannotSetPort(port) => {
                write!(write_init(self, f)?, r#"unable to set port to "{port}""#)
            }
            GetThingDescriptionErrorKind::Request(_) => {
                write_init(self, f)?.write_str("request failed")
            }
            GetThingDescriptionErrorKind::RequestStatus(_) => {
                write_init(self, f)?.write_str("request returned an error status")
            }
            GetThingDescriptionErrorKind::InvalidThing(_) => {
                write_init(self, f)?.write_str("cannot deserialize response as a thing description")
            }
        }
    }
}

impl StdError for GetThingDescriptionError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &self.kind {
            GetThingDescriptionErrorKind::InvalidHost(source) => Some(source),
            GetThingDescriptionErrorKind::Request(source)
            | GetThingDescriptionErrorKind::RequestStatus(source)
            | GetThingDescriptionErrorKind::InvalidThing(source) => Some(source),
            GetThingDescriptionErrorKind::UrlHasPath
            | GetThingDescriptionErrorKind::CannotSetScheme(_)
            | GetThingDescriptionErrorKind::CannotSetPort(_) => None,
        }
    }
}

pub async fn consume<Other>(
    td: &Thing<Other>,
    client: reqwest::Client,
) -> Result<Consumer, ConsumeError>
where
    Other: HasHttpProtocolExtension + ExtendableThing,
{
    let mut schema_definitions =
        td.schema_definitions
            .as_ref()
            .map_or_else(Default::default, |schema_definitions| {
                schema_definitions
                    .iter()
                    .map(|(name, schema)| SchemaDefinition {
                        name: name.clone(),
                        schema: Rc::new(data_schema_without_extensions(schema)),
                    })
                    .collect::<Vec<_>>()
            });
    schema_definitions.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    let schema_definitions = schema_definitions;

    let mut properties = td.properties.as_ref().map_or_else(
        || Ok(Default::default()),
        |properties| {
            properties
                .iter()
                .map(|(property_name, property)| {
                    let PropertyAffordance {
                        interaction:
                            InteractionAffordance {
                                attype,
                                title,
                                titles,
                                description,
                                descriptions,
                                forms,
                                uri_variables,
                                other,
                            },
                        data_schema,
                        observable,
                        other: _,
                    } = property;

                    let mut uri_variables = uri_variables.as_ref().map_or_else(
                        || Ok(Default::default()),
                        |uri_variables| {
                            uri_variables
                                .iter()
                                .map(|(name, schema)| {
                                    let schema =
                                        ScalarDataSchema::try_from(schema).map_err(|err| {
                                            ConsumeError::UriVariable {
                                                property: property_name.clone(),
                                                name: name.clone(),
                                                source: err,
                                            }
                                        })?;

                                    Ok(UriVariable {
                                        name: name.clone(),
                                        schema,
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()
                        },
                    )?;
                    uri_variables.sort_unstable_by(|a, b| a.name.cmp(&b.name));

                    let forms = forms
                        .iter()
                        .filter_map(|form| {
                            let op = match &form.op {
                                DefaultedFormOperations::Default
                                    if data_schema.read_only.not()
                                        && data_schema.write_only.not() =>
                                {
                                    vec![FormOperation::ReadProperty, FormOperation::WriteProperty]
                                }
                                DefaultedFormOperations::Default
                                    if data_schema.write_only.not() =>
                                {
                                    vec![FormOperation::ReadProperty]
                                }
                                DefaultedFormOperations::Default if data_schema.read_only.not() => {
                                    vec![FormOperation::WriteProperty]
                                }
                                DefaultedFormOperations::Default => return None,
                                DefaultedFormOperations::Custom(ops) => ops.clone(),
                            };

                            Some(
                                Form::try_from_td_form(form, op, &schema_definitions).map_err(
                                    |source| ConsumeError::Form {
                                        property: property_name.clone(),
                                        href: form.href.clone(),
                                        source,
                                    },
                                ),
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    let data_schema = data_schema_without_extensions(data_schema);

                    Ok(Property {
                        name: property_name.clone(),
                        observable: observable.unwrap_or(false),
                        forms,
                        uri_variables,
                        data_schema,
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        },
    )?;

    let thing_level_forms = td
        .forms
        .as_ref()
        .map(|forms| {
            forms
                .iter()
                .filter_map(|form| match &form.op {
                    DefaultedFormOperations::Default => {
                        return Some(Err(ThingLevelFormError {
                            href: form.href.clone(),
                            kind: ThingLevelFormErrorKind::InvalidDefault,
                        }))
                    }
                    DefaultedFormOperations::Custom(ops) => {
                        let (&op, ops) = ops.split_first()?;

                        if ops.is_empty().not() {
                            return Some(Err(ThingLevelFormError {
                                href: form.href.clone(),
                                kind: ThingLevelFormErrorKind::MultipleOps,
                            }));
                        }

                        Some(match op {
                            FormOperation::ReadAllProperties
                            | FormOperation::WriteAllProperties
                            | FormOperation::ReadMultipleProperties
                            | FormOperation::WriteMultipleProperties
                            | FormOperation::ObserveAllProperties
                            | FormOperation::UnobserveAllProperties
                            | FormOperation::SubscribeAllEvents
                            | FormOperation::UnsubscribeAllEvents
                            | FormOperation::QueryAllActions => {
                                Form::try_from_td_form(form, [op].as_slice(), &schema_definitions)
                                    .map_err(|source| ThingLevelFormError {
                                        href: form.href.clone(),
                                        kind: ThingLevelFormErrorKind::InvalidForm { op, source },
                                    })
                            }

                            FormOperation::ReadProperty
                            | FormOperation::WriteProperty
                            | FormOperation::ObserveProperty
                            | FormOperation::UnobserveProperty
                            | FormOperation::InvokeAction
                            | FormOperation::QueryAction
                            | FormOperation::CancelAction
                            | FormOperation::SubscribeEvent
                            | FormOperation::UnsubscribeEvent => Err(ThingLevelFormError {
                                href: form.href.clone(),
                                kind: ThingLevelFormErrorKind::InvalidOp(op),
                            }),
                        })
                    }
                })
                .collect::<Result<_, _>>()
        })
        .transpose()
        .map_err(ConsumeError::ThingLevelForm)?
        .unwrap_or_default();

    properties.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    let base = td.base.clone();
    Ok(Consumer {
        base,
        properties,
        client,
        thing_level_forms,
    })
}

#[derive(Debug)]
pub enum ConsumeError {
    UriVariable {
        property: String,
        name: String,
        source: NonScalarDataSchema,
    },
    Form {
        property: String,
        href: String,
        source: InvalidForm,
    },
    ThingLevelForm(ThingLevelFormError),
}

#[derive(Debug)]
pub struct ThingLevelFormError {
    pub href: String,
    pub kind: ThingLevelFormErrorKind,
}

#[derive(Debug)]
pub enum ThingLevelFormErrorKind {
    InvalidDefault,
    MultipleOps,
    InvalidOp(FormOperation),
    InvalidForm {
        op: FormOperation,
        source: InvalidForm,
    },
}

#[derive(Debug)]
pub struct Consumer {
    base: Option<String>,
    properties: Vec<Property>,
    thing_level_forms: Vec<Form>,
    client: reqwest::Client,
    // TODO: other
}

impl Consumer {
    #[inline]
    pub fn property(&self, name: &str) -> Option<PropertyRef<'_>> {
        self.properties
            .binary_search_by(|prop| prop.name.as_str().cmp(name))
            .ok()
            .map(|index| {
                let property = &self.properties[index];
                let base = &self.base;
                let client = &self.client;

                PropertyRef {
                    property,
                    base,
                    client,
                }
            })
    }

    // TODO: select form based on uri variables
    pub async fn properties<T>(&self) -> Result<T, PropertiesError>
    where
        T: DeserializeOwned,
    {
        const EMPTY_DATA_SCHEMA: DataSchema<(), (), ()> = DataSchema {
            attype: None,
            title: None,
            titles: None,
            constant: None,
            default: None,
            description: None,
            descriptions: None,
            enumeration: None,
            format: None,
            one_of: None,
            read_only: false,
            write_only: false,
            subtype: None,
            unit: None,
            other: (),
        };

        let form = self
            .thing_level_forms
            .iter()
            .find(|form| form.op == [FormOperation::ReadAllProperties])
            .ok_or(PropertiesError::MissingForm)?;

        // TODO: handle uri variables
        // TODO: handle security
        // TODO: create a dataschema for the properties
        // TODO: validate response
        create_request_future(
            form,
            &self.base,
            &[],
            &None,
            &EMPTY_DATA_SCHEMA,
            &self.client,
        )
        .map_err(|source| PropertiesError::CreateRequest {
            href: form.href.clone(),
            source,
        })?
        .await
        .map_err(|source| PropertiesError::Request {
            href: form.href.clone(),
            source,
        })
    }
}

#[derive(Debug)]
pub enum PropertiesError {
    MissingForm,
    CreateRequest {
        href: String,
        source: CreateRequestFutureError,
    },
    Request {
        href: String,
        source: PerformRequestError,
    },
}

impl Display for PropertiesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl StdError for PropertiesError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        todo!()
    }
}

#[derive(Debug)]
pub struct PropertyRef<'a> {
    property: &'a Property,
    base: &'a Option<String>,
    client: &'a reqwest::Client,
}

#[derive(Debug)]
struct Property {
    name: String,
    observable: bool,
    forms: Vec<Form>,
    uri_variables: Vec<UriVariable>,
    data_schema: DataSchema<(), (), ()>,
}

impl<'a> PropertyRef<'a> {
    pub fn read<T>(&self) -> PropertyReader<'a, T> {
        let inner = self.create_inner();
        PropertyReader {
            status: PropertyReaderWriterStatus(Ok(inner)),
            _marker: PhantomData,
        }
    }

    pub fn write<T>(&'a self, value: &'a T) -> PropertyWriter<'a, T> {
        let inner = self.create_inner();
        PropertyWriter {
            status: PropertyReaderWriterStatus(Ok(inner)),
            value,
        }
    }

    #[inline]
    fn create_inner(&self) -> PropertyReaderWriterInner<'a> {
        PropertyReaderWriterInner {
            property: self.property,
            forms: PartialVecRefs::Whole(&self.property.forms),
            uri_variables: Vec::new(),
            security: None,
            base: self.base,
            client: self.client,
        }
    }
}

#[derive(Debug)]
struct UriVariable {
    name: String,
    schema: ScalarDataSchema,
}

#[derive(Debug, Clone)]
struct Form {
    href: String,
    op: Vec<FormOperation>,
    content_type: Option<String>,
    content_coding: Option<String>,
    subprotocol: Option<Subprotocol>,
    security_scheme: SecurityScheme,
    expected_response_content_type: Option<String>,
    additional_expected_response: Option<Vec<AdditionalExpectedResponse>>,
    method_name: Option<http::Method>,
}

impl Form {
    fn try_from_td_form<Other>(
        form: &thing::Form<Other>,
        op: impl Into<Vec<FormOperation>>,
        schema_definitions: &[SchemaDefinition],
    ) -> Result<Self, InvalidForm>
    where
        Other: HasHttpProtocolExtension,
    {
        let thing::Form {
            op: _,
            href,
            content_type,
            content_coding,
            subprotocol,
            security,
            scopes,
            response,
            additional_responses,
            other,
        } = form;

        let content_type = content_type.clone();
        let content_coding = content_coding.clone();
        let subprotocol = subprotocol
            .as_deref()
            .map(|subprotocol| {
                Subprotocol::try_from(subprotocol).map_err(InvalidForm::UnsupportedSubprotocol)
            })
            .transpose()?;
        let security_scheme = security
            .as_deref()
            .try_into()
            .map_err(InvalidForm::UnsupportedSecurity)?;
        let expected_response_content_type = response
            .as_ref()
            .map(|response| response.content_type.clone());
        let additional_expected_response = additional_responses
            .as_ref()
            .map(|additional_responses| {
                additional_responses
                    .iter()
                    .map(|additional_response| {
                        AdditionalExpectedResponse::try_from_td_additional_response(
                            additional_response,
                            schema_definitions,
                        )
                        .map_err(InvalidForm::MissingSchemaDefinition)
                    })
                    .collect::<Result<_, _>>()
            })
            .transpose()?;
        let href = href.clone();
        let method_name = Other::http_protocol_form(other).method_name;
        let op = op.into();

        Ok(Self {
            href,
            op,
            content_type,
            content_coding,
            subprotocol,
            security_scheme,
            expected_response_content_type,
            additional_expected_response,
            method_name,
        })
    }
}

#[derive(Debug)]
pub enum InvalidForm {
    UnsupportedProtocol(String),
    UnsupportedSubprotocol(UnsupportedSubprotocol),
    UnsupportedSecurity(UnsupportedSecuritySchemeSet),
    MissingSchemaDefinition(MissingSchemaDefinition),
}

/// A fixed set of supported protocols.
///
/// **Warning**: this will be removed in the future, in order to support arbitrary subprotocol that
/// can be correctly handled by the users using traits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Subprotocol {
    Sse,
    LongPolling,
}

#[derive(Debug)]
pub struct UnsupportedSubprotocol(pub String);

impl TryFrom<&str> for Subprotocol {
    type Error = UnsupportedSubprotocol;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value {
            "sse" => Self::Sse,
            "longpolling" => Self::LongPolling,
            _ => return Err(UnsupportedSubprotocol(value.to_owned())),
        })
    }
}

impl FromStr for Subprotocol {
    type Err = UnsupportedSubprotocol;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct SecurityScheme: u8 {
        const NO_SECURITY = 0b001;
        const BASIC = 0b010;
        const BEARER = 0b100;
    }
}

impl TryFrom<Option<&[String]>> for SecurityScheme {
    type Error = UnsupportedSecuritySchemeSet;

    fn try_from(value: Option<&[String]>) -> Result<Self, Self::Error> {
        value.map_or(Ok(Self::NO_SECURITY), |security_set| {
            security_set
                .iter()
                .filter_map(|security| {
                    Some(match security.as_str() {
                        "nosec" => Self::NO_SECURITY,
                        "basic" => Self::BASIC,
                        "bearer" => Self::BEARER,
                        _ => return None,
                    })
                })
                .reduce(BitOr::bitor)
                .ok_or_else(|| UnsupportedSecuritySchemeSet(security_set.to_vec()))
        })
    }
}

#[derive(Debug)]
pub struct UnsupportedSecuritySchemeSet(pub Vec<String>);

#[derive(Debug, Clone)]
pub struct AdditionalExpectedResponse {
    success: bool,
    content_type: Option<String>,
    // TODO: should we use an `Arc`? Should be weak?
    schema: Option<Rc<DataSchema<(), (), ()>>>,
}

impl AdditionalExpectedResponse {
    fn try_from_td_additional_response(
        other: &thing::AdditionalExpectedResponse,
        schema_definitions: &[SchemaDefinition],
    ) -> Result<Self, MissingSchemaDefinition> {
        let &thing::AdditionalExpectedResponse {
            success,
            ref content_type,
            ref schema,
        } = other;

        let schema = schema
            .as_ref()
            .map(|schema| {
                schema_definitions
                    .binary_search_by_key(&schema.as_str(), |schema_definition| {
                        schema_definition.name.as_str()
                    })
                    .map(|index| Rc::clone(&schema_definitions[index].schema))
                    .map_err(|_| MissingSchemaDefinition(schema.clone()))
            })
            .transpose()?;

        let content_type = content_type.clone();
        Ok(Self {
            success,
            content_type,
            schema,
        })
    }
}

#[derive(Debug)]
pub struct MissingSchemaDefinition(pub String);

#[derive(Debug)]
pub struct ScalarDataSchema {
    constant: Option<serde_json::Value>,
    default: Option<serde_json::Value>,
    unit: Option<String>,
    one_of: Option<Vec<Self>>,
    enumeration: Option<Vec<serde_json::Value>>,
    read_only: bool,
    write_only: bool,
    format: Option<String>,
    subtype: Option<ScalarDataSchemaSubtype>,
}

impl<DS, AS, OS> TryFrom<&DataSchema<DS, AS, OS>> for ScalarDataSchema {
    type Error = NonScalarDataSchema;

    fn try_from(value: &DataSchema<DS, AS, OS>) -> Result<Self, Self::Error> {
        let DataSchema {
            attype: _,
            title: _,
            titles: _,
            description: _,
            descriptions: _,
            constant,
            default,
            unit,
            one_of,
            enumeration,
            read_only,
            write_only,
            format,
            subtype,
            other: _,
        } = value;

        let subtype = subtype.as_ref().map(TryInto::try_into).transpose()?;

        let constant = constant.clone();
        let default = default.clone();
        let unit = unit.clone();
        let one_of = one_of
            .as_ref()
            .map(|one_of| one_of.iter().map(Self::try_from).collect())
            .transpose()?;
        let enumeration = enumeration.clone();
        let read_only = *read_only;
        let write_only = *write_only;
        let format = format.clone();

        Ok(Self {
            constant,
            default,
            unit,
            one_of,
            enumeration,
            read_only,
            write_only,
            format,
            subtype,
        })
    }
}

#[derive(Debug)]
pub struct NonScalarDataSchema;

#[derive(Debug)]
enum ScalarDataSchemaSubtype {
    Boolean,
    Number(NumberSchema),
    Integer(IntegerSchema),
    String(StringSchema),
    Null,
}

impl<DS, AS, OS> TryFrom<&DataSchemaSubtype<DS, AS, OS>> for ScalarDataSchemaSubtype {
    type Error = NonScalarDataSchema;

    fn try_from(value: &DataSchemaSubtype<DS, AS, OS>) -> Result<Self, Self::Error> {
        Ok(match value {
            DataSchemaSubtype::Boolean => ScalarDataSchemaSubtype::Boolean,
            DataSchemaSubtype::Number(x) => ScalarDataSchemaSubtype::Number(x.clone()),
            DataSchemaSubtype::Integer(x) => ScalarDataSchemaSubtype::Integer(x.clone()),
            DataSchemaSubtype::String(x) => ScalarDataSchemaSubtype::String(x.clone()),
            DataSchemaSubtype::Null => ScalarDataSchemaSubtype::Null,
            DataSchemaSubtype::Object(_) | DataSchemaSubtype::Array(_) => {
                return Err(NonScalarDataSchema)
            }
        })
    }
}

#[derive(Debug)]
struct SchemaDefinition {
    name: String,
    schema: Rc<DataSchema<(), (), ()>>,
}

fn data_schema_without_extensions<DS, AS, OS>(
    data_schema: &DataSchema<DS, AS, OS>,
) -> DataSchema<(), (), ()> {
    let DataSchema {
        attype,
        title,
        titles,
        description,
        descriptions,
        constant,
        default,
        unit,
        one_of,
        enumeration,
        read_only,
        write_only,
        format,
        subtype,
        other: _,
    } = data_schema;

    let attype = attype.clone();
    let title = title.clone();
    let titles = titles.clone();
    let description = description.clone();
    let descriptions = descriptions.clone();
    let constant = constant.clone();
    let default = default.clone();
    let unit = unit.clone();
    let one_of = one_of
        .as_ref()
        .map(|one_of| one_of.iter().map(data_schema_without_extensions).collect());
    let enumeration = enumeration.clone();
    let read_only = *read_only;
    let write_only = *write_only;
    let format = format.clone();
    let subtype = subtype.as_ref().map(data_schema_subtype_without_extensions);

    DataSchema {
        attype,
        title,
        titles,
        description,
        descriptions,
        constant,
        default,
        unit,
        one_of,
        enumeration,
        read_only,
        write_only,
        format,
        subtype,
        other: (),
    }
}

fn data_schema_subtype_without_extensions<DS, AS, OS>(
    subtype: &DataSchemaSubtype<DS, AS, OS>,
) -> DataSchemaSubtype<(), (), ()> {
    match subtype {
        DataSchemaSubtype::Array(array) => {
            let ArraySchema {
                items,
                min_items,
                max_items,
                other: _,
            } = array;

            let items = items
                .as_ref()
                .map(|items| items.to_ref().map(data_schema_without_extensions));
            let min_items = *min_items;
            let max_items = *max_items;

            DataSchemaSubtype::Array(ArraySchema {
                items,
                min_items,
                max_items,
                other: (),
            })
        }
        DataSchemaSubtype::Boolean => DataSchemaSubtype::Boolean,
        DataSchemaSubtype::Number(number) => DataSchemaSubtype::Number(number.clone()),
        DataSchemaSubtype::Integer(integer) => DataSchemaSubtype::Integer(integer.clone()),
        DataSchemaSubtype::Object(object) => {
            let ObjectSchema {
                properties,
                required,
                other: _,
            } = object;
            let properties = properties.as_ref().map(|properties| {
                properties
                    .iter()
                    .map(|(name, schema)| {
                        let name = name.clone();
                        let schema = data_schema_without_extensions(schema);
                        (name, schema)
                    })
                    .collect()
            });

            let required = required.clone();

            DataSchemaSubtype::Object(ObjectSchema {
                properties,
                required,
                other: (),
            })
        }
        DataSchemaSubtype::String(s) => DataSchemaSubtype::String(s.clone()),
        DataSchemaSubtype::Null => DataSchemaSubtype::Null,
    }
}

#[derive(Debug)]
pub struct PropertyReader<'a, T> {
    status: PropertyReaderWriterStatus<'a>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T> PropertyReader<'a, T> {
    #[inline]
    pub fn no_security(&mut self) -> &mut Self {
        self.status.no_security();
        self
    }

    #[inline]
    pub fn basic(&mut self, username: impl Into<String>, password: impl Into<String>) -> &mut Self {
        self.status.basic(username, password);
        self
    }

    #[inline]
    pub fn protocol(&mut self, accepted_protocol: Protocol) -> &mut Self {
        self.status.protocol(accepted_protocol);
        self
    }

    #[inline]
    pub fn protocols(&mut self, accepted_protocols: &[Protocol]) -> &mut Self {
        self.status.protocols(accepted_protocols);
        self
    }

    #[inline]
    pub fn bearer(&mut self, token: impl Into<String>) -> &mut Self {
        self.status.bearer(token);
        self
    }

    #[inline]
    pub fn uri_variable<N>(&mut self, name: N, value: UriVariableValue) -> &mut Self
    where
        N: Into<String> + AsRef<str>,
    {
        self.status.uri_variable(name, value);
        self
    }

    pub fn send(
        &self,
    ) -> Result<
        impl Future<Output = Result<T, PerformRequestError>> + '_,
        PropertyReaderSendError<'_>,
    >
    where
        T: DeserializeOwned,
    {
        let PropertyReaderWriterInner {
            property,
            base,
            forms,
            uri_variables,
            security,
            client,
        } = self
            .status
            .0
            .as_ref()
            .map_err(|err| PropertyReaderSendError::PropertyReader(err.clone()))?;

        let mut forms = forms
            .iter()
            .filter(|form| form.op.contains(&FormOperation::ReadProperty));
        let form = forms.next().ok_or(PropertyReaderSendError::NoForms)?;
        if forms.next().is_some() {
            return Err(PropertyReaderSendError::MultipleChoices);
        }

        create_request_future(
            form,
            base,
            uri_variables,
            security,
            &property.data_schema,
            client,
        )
        .map_err(PropertyReaderSendError::InitFuture)
    }
}

#[derive(Debug)]
pub struct PropertyReaderWriterStatus<'a>(
    Result<PropertyReaderWriterInner<'a>, PropertyReaderWriterError<'a>>,
);

impl<'a> PropertyReaderWriterStatus<'a> {
    pub fn no_security(&mut self) {
        self.filter_security_scheme(StatefulSecurityScheme::NoSecurity)
    }

    pub fn basic(&mut self, username: impl Into<String>, password: impl Into<String>) {
        let username = username.into();
        let password = password.into();

        self.filter_security_scheme(StatefulSecurityScheme::Basic { username, password })
    }

    pub fn bearer(&mut self, token: impl Into<String>) {
        let token = token.into();

        self.filter_security_scheme(StatefulSecurityScheme::Bearer(token))
    }

    #[inline]
    pub fn protocol(&mut self, accepted_protocol: Protocol) {
        self.protocols([accepted_protocol].as_slice())
    }

    pub fn protocols(&mut self, accepted_protocols: &[Protocol]) {
        if let Ok(inner) = &mut self.0 {
            let base_protocol = inner
                .base
                .as_deref()
                .and_then(|base| base.split_once("://"))
                .and_then(|(schema, _)| schema.parse::<Protocol>().ok())
                .filter(|protocol| accepted_protocols.contains(protocol));

            inner.forms.retain(|form| {
                form.href
                    .split_once("://")
                    .map(|(schema, _)| schema.parse::<Protocol>().ok())
                    .map_or(base_protocol, identity)
                    .map_or(false, |protocol| accepted_protocols.contains(&protocol))
            });

            if inner.forms.is_empty() {
                self.0 = Err(PropertyReaderWriterError {
                    property: inner.property,
                    kind: PropertyReaderWriterErrorKind::UnavailableProtocols,
                });
            }
        }
    }

    // TODO: check if we can make `value: impl TryInto<UriVariableValue>`, the type of error could
    // be annyoing because of the `Infallible` conversions.
    pub fn uri_variable<N>(&mut self, name: N, value: UriVariableValue)
    where
        N: Into<String> + AsRef<str>,
    {
        let Ok(inner) = &mut self.0 else {
            return;
        };

        let name_str = name.as_ref();
        let uri_variable = inner
            .property
            .uri_variables
            .binary_search_by(|uri_variable| uri_variable.name.as_str().cmp(name_str))
            .map(|index| &inner.property.uri_variables[index]);

        let Ok(uri_variable) = uri_variable else {
            self.0 = Err(PropertyReaderWriterError {
                property: inner.property,
                kind: PropertyReaderWriterErrorKind::UnavailableUriVariable(name.into()),
            });
            return;
        };

        if let Err(err) = validate_uri_variable_value(&uri_variable.schema, &value) {
            self.0 = Err(PropertyReaderWriterError {
                property: inner.property,
                kind: PropertyReaderWriterErrorKind::InvalidUriVariable {
                    uri_variable: uri_variable.name.clone(),
                    kind: err,
                },
            });
            return;
        };

        inner.uri_variables.push((name.into(), value));
    }

    fn filter_security_scheme(&mut self, security_scheme: StatefulSecurityScheme) {
        if let Ok(inner) = &mut self.0 {
            if inner.security.is_none() {
                inner
                    .forms
                    .retain(|form| form.security_scheme.contains(security_scheme.to_flag()));

                if inner.forms.is_empty() {
                    self.0 = Err(PropertyReaderWriterError {
                        property: inner.property,
                        kind: PropertyReaderWriterErrorKind::UnavailableSecurityScheme(
                            security_scheme.to_stateless(),
                        ),
                    });
                } else {
                    inner.security = Some(security_scheme);
                }
            } else {
                self.0 = Err(PropertyReaderWriterError {
                    property: inner.property,
                    kind: PropertyReaderWriterErrorKind::SecurityAlreadySet,
                });
            }
        }
    }
}

#[derive(Debug)]
struct PropertyReaderWriterInner<'a> {
    property: &'a Property,
    base: &'a Option<String>,
    forms: PartialVecRefs<'a, Form>,
    uri_variables: Vec<(String, UriVariableValue)>,
    security: Option<StatefulSecurityScheme>,
    client: &'a reqwest::Client,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatelessSecurityScheme {
    #[default]
    NoSecurity,
    Basic,
    Bearer,
}

#[derive(Debug, Default, Zeroize, ZeroizeOnDrop)]
enum StatefulSecurityScheme {
    #[default]
    NoSecurity,
    Basic {
        username: String,
        password: String,
    },
    Bearer(String),
}

impl StatefulSecurityScheme {
    fn to_stateless(&self) -> StatelessSecurityScheme {
        match self {
            Self::NoSecurity => StatelessSecurityScheme::Basic,
            Self::Basic { .. } => StatelessSecurityScheme::Basic,
            Self::Bearer(_) => StatelessSecurityScheme::Bearer,
        }
    }

    fn to_flag(&self) -> SecurityScheme {
        match self {
            Self::NoSecurity => SecurityScheme::NO_SECURITY,
            Self::Basic { .. } => SecurityScheme::BASIC,
            Self::Bearer(_) => SecurityScheme::BEARER,
        }
    }
}

#[derive(Debug)]
enum PartialVecRefs<'a, T> {
    Whole(&'a [T]),
    Some(Vec<&'a T>),
}

impl<'a, T> PartialVecRefs<'a, T> {
    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&&T) -> bool,
    {
        match self {
            Self::Whole(data) => {
                if let Some(first_to_remove) = data.iter().position(|element| f(&element).not()) {
                    let partial = data[..(first_to_remove.saturating_sub(1))]
                        .iter()
                        .chain(data[(first_to_remove + 1)..].iter().filter(f))
                        .collect();

                    *self = Self::Some(partial);
                }
            }
            Self::Some(data) => data.retain(f),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Whole(data) => data.is_empty(),
            Self::Some(data) => data.is_empty(),
        }
    }

    fn iter(&'a self) -> <&'a Self as IntoIterator>::IntoIter {
        IntoIterator::into_iter(self)
    }
}

impl<'a, T> IntoIterator for &'a PartialVecRefs<'a, T> {
    type Item = &'a T;
    type IntoIter = PartialVecRefsIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            PartialVecRefs::Whole(data) => PartialVecRefsIter::Whole(data.iter()),
            PartialVecRefs::Some(data) => PartialVecRefsIter::Some(data.iter()),
        }
    }
}

#[derive(Debug)]
enum PartialVecRefsIter<'a, T> {
    Whole(slice::Iter<'a, T>),
    Some(slice::Iter<'a, &'a T>),
}

impl<'a, T> Iterator for PartialVecRefsIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Whole(iter) => iter.next(),
            Self::Some(iter) => iter.next().copied(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Whole(iter) => iter.size_hint(),
            Self::Some(iter) => iter.size_hint(),
        }
    }
}

impl<T> DoubleEndedIterator for PartialVecRefsIter<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Whole(iter) => iter.next_back(),
            Self::Some(iter) => iter.next_back().copied(),
        }
    }
}

impl<T> ExactSizeIterator for PartialVecRefsIter<'_, T> {
    fn len(&self) -> usize {
        match self {
            Self::Whole(iter) => iter.len(),
            Self::Some(iter) => iter.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PropertyReaderWriterError<'a> {
    property: &'a Property,
    pub kind: PropertyReaderWriterErrorKind,
}

#[derive(Debug, Clone)]
pub enum PropertyReaderWriterErrorKind {
    UnavailableSecurityScheme(StatelessSecurityScheme),
    UnavailableProtocols,
    SecurityAlreadySet,
    UnavailableUriVariable(String),
    InvalidUriVariable {
        uri_variable: String,
        kind: InvalidUriVariableKind,
    },
}

#[derive(Debug, PartialEq)]
pub enum UriVariableValue {
    String(String),
    Integer(i64),
    Number(f64),
    Boolean(bool),
}

impl PartialEq<serde_json::Value> for UriVariableValue {
    fn eq(&self, other: &serde_json::Value) -> bool {
        use serde_json::Value;

        match (self, other) {
            (Self::String(a), Value::String(b)) => a == b,
            (Self::Integer(a), Value::Number(b)) => b.as_i64().map_or(false, |b| a == &b),
            (Self::Number(a), Value::Number(b)) => b.as_f64().map_or(false, |b| a == &b),
            (Self::Boolean(a), Value::Bool(b)) => a == b,
            _ => false,
        }
    }
}

impl Display for UriVariableValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(s) => f.write_str(s),
            Self::Integer(i) => write!(f, "{}", i),
            Self::Number(n) => write!(f, "{}", n),
            Self::Boolean(b) => write!(f, "{}", b),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    Http,
    Https,
}

impl Protocol {
    #[inline]
    pub fn to_str(&self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

#[derive(Debug)]
pub struct UnsupportedProtocol(pub String);

impl FromStr for Protocol {
    type Err = UnsupportedProtocol;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "http" => Self::Http,
            "https" => Self::Https,
            _ => return Err(UnsupportedProtocol(s.to_owned())),
        })
    }
}

fn create_request_future<'a, T>(
    form: &Form,
    base: &Option<String>,
    uri_variables: &[(String, UriVariableValue)],
    security: &'a Option<StatefulSecurityScheme>,
    data_schema: &'a DataSchema<(), (), ()>,
    client: &reqwest::Client,
) -> Result<impl Future<Output = Result<T, PerformRequestError>> + 'a, CreateRequestFutureError>
where
    T: DeserializeOwned + 'a,
{
    let url = form
        .href
        .parse::<Url>()
        .or_else(|err| match err {
            url::ParseError::RelativeUrlWithoutBase => base
                .as_ref()
                .ok_or_else(|| CreateRequestFutureError::RelativeUrlWithoutBase(form.href.clone()))
                .and_then(|base| {
                    base.parse::<Url>()
                        .map_err(|source| CreateRequestFutureError::InvalidBase {
                            base: base.to_string(),
                            href: form.href.clone(),
                            source,
                        })
                })
                .and_then(|base| {
                    base.join(&form.href).map_err(|source| {
                        CreateRequestFutureError::InvalidJoinedHref {
                            base,
                            href: form.href.clone(),
                            source,
                        }
                    })
                }),
            _ => Err(CreateRequestFutureError::InvalidHref {
                href: form.href.clone(),
                source: err,
            }),
        })
        .map(|mut url| {
            use std::fmt::Write;

            if uri_variables.is_empty().not() {
                let mut query_pairs = url.query_pairs_mut();
                let mut buffer = String::new();

                for (name, value) in uri_variables {
                    buffer.clear();
                    write!(buffer, "{}", value).unwrap();
                    query_pairs.append_pair(name, &buffer);
                }
            }
            url
        })?;

    let method = form.method_name.map_or(reqwest::Method::GET, |method| {
        use http::Method;
        match method {
            Method::Get => reqwest::Method::GET,
            Method::Put => reqwest::Method::PUT,
            Method::Post => reqwest::Method::POST,
            Method::Delete => reqwest::Method::DELETE,
            Method::Patch => reqwest::Method::PATCH,
        }
    });

    let builder = client.request(method, url);
    let builder = match security {
        None | Some(StatefulSecurityScheme::NoSecurity) => builder,
        Some(StatefulSecurityScheme::Basic { username, password }) => {
            builder.basic_auth(username, Some(password))
        }
        Some(StatefulSecurityScheme::Bearer(token)) => builder.bearer_auth(token),
    };

    // FIXME: unify the two condition in order to store (at least try to) an impl Future
    // instead of a pin-boxed dyn Future
    match &form.content_type {
        Some(content_type) if content_type.eq_ignore_ascii_case("application/json") => {
            Ok(perform_request(builder, handle_json_response, data_schema))
        }
        None => Ok(perform_request(builder, handle_json_response, data_schema)),
        Some(content_type) => Err(CreateRequestFutureError::InvalidResponse(
            PerformRequestInvalidResponse::UnsupportedFormat(content_type.clone()),
        )),
    }
}

async fn perform_request<'a, T, F, Fut>(
    builder: reqwest::RequestBuilder,
    response_handler: F,
    data_schema: &'a DataSchema<(), (), ()>,
) -> Result<T, PerformRequestError>
where
    F: 'a + FnOnce(reqwest::Response, &'a DataSchema<(), (), ()>) -> Fut,
    Fut: 'a + Future<Output = Result<T, PerformRequestInvalidResponse>>,
{
    let response = builder
        .send()
        .await
        .map_err(PerformRequestError::RequestError)?
        .error_for_status()
        .map_err(PerformRequestError::ResponseError)?;

    response_handler(response, data_schema)
        .await
        .map_err(PerformRequestError::InvalidResponse)
}

#[derive(Debug)]
pub enum PropertyReaderSendError<'a> {
    PropertyReader(PropertyReaderWriterError<'a>),
    MultipleChoices,
    NoForms,
    InitFuture(CreateRequestFutureError),
}

#[derive(Debug)]
pub enum CreateRequestFutureError {
    InvalidHref {
        href: String,
        source: url::ParseError,
    },
    RelativeUrlWithoutBase(String),
    InvalidBase {
        base: String,
        href: String,
        source: url::ParseError,
    },
    InvalidJoinedHref {
        base: Url,
        href: String,
        source: url::ParseError,
    },
    InvalidResponse(PerformRequestInvalidResponse),
}

#[derive(Debug)]
pub enum PerformRequestError {
    InvalidResponse(PerformRequestInvalidResponse),
    RequestError(reqwest::Error),
    ResponseError(reqwest::Error),
}

#[derive(Debug)]
pub enum PerformRequestInvalidResponse {
    Json(HandleJsonResponseError),
    UnsupportedFormat(String),
}

#[inline]
async fn handle_json_response<T>(
    response: reqwest::Response,
    data_schema: &DataSchema<(), (), ()>,
) -> Result<T, PerformRequestInvalidResponse>
where
    T: DeserializeOwned,
{
    handle_json_response_inner(response, data_schema)
        .await
        .map_err(PerformRequestInvalidResponse::Json)
}

async fn handle_json_response_inner<T>(
    response: reqwest::Response,
    data_schema: &DataSchema<(), (), ()>,
) -> Result<T, HandleJsonResponseError>
where
    T: DeserializeOwned,
{
    let data: serde_json::Value = response
        .json()
        .await
        .map_err(HandleJsonResponseError::Json)?;

    handle_json_response_validate(json::ValueRef(&data), data_schema)
        .map_err(HandleJsonResponseRefError::into_owned)?;
    serde_json::from_value(data).map_err(HandleJsonResponseError::Deserialization)
}

#[derive(Debug)]
struct ResponseValidatorState<'a> {
    responses: json::ValueRefs<'a>,
    branching: ResponseValidatorBranching<'a>,
    logic: ResponseValidatorLogic,
}

fn handle_json_response_validate<'a>(
    response: json::ValueRef<'a>,
    data_schema: &'a DataSchema<(), (), ()>,
) -> Result<(), HandleJsonResponseRefError<'a>> {
    let mut queue: SmallVec<[_; 8]> = smallvec![ResponseValidatorState {
        responses: response.to_refs(),
        branching: ResponseValidatorBranching::Unchecked(data_schema),
        logic: ResponseValidatorLogic::And { parent: None },
    }];

    while let Some(ResponseValidatorState {
        responses,
        branching,
        mut logic,
    }) = queue.pop()
    {
        match branching {
            ResponseValidatorBranching::Unchecked(data_schema) => {
                if data_schema.write_only {
                    handle_response_validate_error(
                        HandleJsonResponseRefError::WriteOnly,
                        logic,
                        &mut queue,
                    )?;
                    continue;
                }

                // Push eventual one_of before other validators, in order to pop it later
                if let Some(one_of) = &data_schema.one_of {
                    queue.push(ResponseValidatorState {
                        responses,
                        branching: ResponseValidatorBranching::UnevaluatedOneOf(one_of),
                        logic,
                    });
                }

                let evaluated_index = queue.len();
                queue.push(ResponseValidatorState {
                    responses,
                    branching: ResponseValidatorBranching::Evaluated(Some(data_schema)),
                    logic,
                });

                // From last to first, in order to pop from the first to last
                if let Err(err) = responses.0.iter().rev().try_for_each(|response| {
                    handle_json_response_validate_impl(response.into(), data_schema, &mut queue)
                }) {
                    queue.truncate(evaluated_index);
                    handle_response_validate_error(err, logic, &mut queue)?;
                }
            }

            ResponseValidatorBranching::Evaluated(..) => {
                if let ResponseValidatorLogic::Or { parent } = logic {
                    // Parent is an evaluated `one_of`, if we reached this point we satisfied one of
                    // its conditions, thus we need to remove it and its subsequent elements.
                    queue.truncate(parent);
                }
            }

            ResponseValidatorBranching::UnevaluatedOneOf(one_of) => {
                let (first_response, other_responses) = responses
                    .split_first()
                    .expect("at least one response should be always available");

                let mut parent = queue.len();
                if other_responses.0.is_empty().not() {
                    queue.push(ResponseValidatorState {
                        responses,
                        branching: ResponseValidatorBranching::Evaluated(None),
                        logic,
                    });

                    logic = ResponseValidatorLogic::And {
                        parent: Some(parent),
                    };
                    // From last to first, in order to perform pops from first to last
                    queue.extend(other_responses.0.iter().rev().map(|response| {
                        ResponseValidatorState {
                            responses: ValueRef(response).to_refs(),
                            branching: ResponseValidatorBranching::UnevaluatedOneOf(one_of),
                            logic,
                        }
                    }));

                    parent = queue.len();
                }

                queue.push(ResponseValidatorState {
                    responses,
                    branching: ResponseValidatorBranching::EvaluatedOneOf(one_of),
                    logic,
                });

                // From last to first, in order to perform pops from first to last
                queue.extend(
                    one_of
                        .iter()
                        .rev()
                        .map(|data_schema| ResponseValidatorState {
                            responses: first_response.to_refs(),
                            branching: ResponseValidatorBranching::Unchecked(data_schema),
                            logic: ResponseValidatorLogic::Or { parent },
                        }),
                );
            }

            ResponseValidatorBranching::EvaluatedOneOf(one_of) => {
                handle_response_validate_error(
                    HandleJsonResponseRefError::OneOf {
                        expected: one_of,
                        found: response.0,
                    },
                    logic,
                    &mut queue,
                )?;
            }
        }
    }

    Ok(())
}

fn handle_response_validate_error<'a, const N: usize>(
    error: HandleJsonResponseRefError<'a>,
    mut logic: ResponseValidatorLogic,
    queue: &mut SmallVec<[ResponseValidatorState; N]>,
) -> Result<(), HandleJsonResponseRefError<'a>> {
    loop {
        match logic {
            ResponseValidatorLogic::Or { .. } => break Ok(()),
            ResponseValidatorLogic::And { parent: None } => break Err(error),
            ResponseValidatorLogic::And {
                parent: Some(parent),
            } => {
                // TODO: find a way to create an error chain to give useful information to the user
                // without sacrificing performances
                logic = queue[parent].logic;
                queue.truncate(parent);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseValidatorLogic {
    And { parent: Option<usize> },
    Or { parent: usize },
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ResponseValidatorBranching<'a> {
    Unchecked(&'a DataSchema<(), (), ()>),
    Evaluated(Option<&'a DataSchema<(), (), ()>>),
    UnevaluatedOneOf(&'a [DataSchema<(), (), ()>]),
    EvaluatedOneOf(&'a [DataSchema<(), (), ()>]),
}

fn handle_json_response_validate_impl<'a, const N: usize>(
    response: json::ValueRef<'a>,
    data_schema: &'a DataSchema<(), (), ()>,
    queue: &mut SmallVec<[ResponseValidatorState<'a>; N]>,
) -> Result<(), HandleJsonResponseRefError<'a>> {
    if let Some(constant) = &data_schema.constant {
        if response != constant {
            return Err(HandleJsonResponseRefError::Constant {
                expected: constant,
                found: response.0,
            });
        }
    }

    if let Some(enumeration) = &data_schema.enumeration {
        if enumeration.iter().any(|variant| response == variant).not() {
            return Err(HandleJsonResponseRefError::Enumeration {
                expected: enumeration,
                found: response.0,
            });
        }
    }

    let parent = queue.len() - 1;
    debug_assert!(matches!(
        queue[parent].branching,
        ResponseValidatorBranching::Evaluated(Some(queue_data_schema))
        if std::ptr::eq(queue_data_schema, data_schema)
    ));
    let parent = Some(parent);

    if let Some(subtype) = &data_schema.subtype {
        use serde_json::Value;

        match (subtype, response.0) {
            (DataSchemaSubtype::Boolean, Value::Bool(_))
            | (DataSchemaSubtype::Null, Value::Null) => {}

            (DataSchemaSubtype::Array(data_schema), Value::Array(responses)) => {
                if let Some(min_items) = data_schema.min_items {
                    if u32::try_from(responses.len()).map_or(false, |len| len < min_items) {
                        return Err(HandleJsonResponseRefError::ArraySubtype(
                            ArraySubtypeError::MinItems {
                                expected: min_items,
                                found: responses.len(),
                            },
                        ));
                    }
                }

                if let Some(max_items) = data_schema.max_items {
                    if u32::try_from(responses.len()).map_or(true, |len| len > max_items) {
                        return Err(HandleJsonResponseRefError::ArraySubtype(
                            ArraySubtypeError::MaxItems {
                                expected: max_items,
                                found: responses.len(),
                            },
                        ));
                    }
                }

                if let Some(items) = &data_schema.items {
                    match items {
                        BoxedElemOrVec::Elem(elem) => queue.push(ResponseValidatorState {
                            responses: responses.as_slice().into(),
                            branching: ResponseValidatorBranching::Unchecked(elem.as_ref()),
                            logic: ResponseValidatorLogic::And { parent },
                        }),
                        BoxedElemOrVec::Vec(data_schemas) => {
                            // Note: we can have a different number of responses and data schemas.
                            //
                            // If the number of responses is less than the data schemas, it is ok
                            // (unless against min_items). Moreover, we can have more responses
                            // than data schemas: we MUST not validate the additional elements
                            // according to the spec. In fact, we have to handle the validation
                            // like `items: true` in 2020-12 JSON schema draft.
                            queue.extend(data_schemas.iter().zip(responses.iter()).rev().map(
                                |(data_schema, response)| ResponseValidatorState {
                                    responses: json::ValueRef::from(response).to_refs(),
                                    branching: ResponseValidatorBranching::Unchecked(data_schema),
                                    logic: ResponseValidatorLogic::And { parent },
                                },
                            ));
                        }
                    }
                }
            }

            (DataSchemaSubtype::Number(data_schema), Value::Number(response)) => {
                #[inline]
                fn convert<T>(x: impl TryInto<T>) -> Option<f64>
                where
                    f64: From<T>,
                {
                    x.try_into().ok().map(f64::from)
                }

                let value = response
                    .as_u64()
                    .map(convert::<u32>)
                    .or_else(|| response.as_i64().map(convert::<i32>))
                    .unwrap_or_else(|| response.as_f64())
                    .ok_or_else(|| {
                        HandleJsonResponseRefError::NumberSubtype(NumericSubtypeError::InvalidType(
                            response.clone(),
                        ))
                    })?;

                check_number(data_schema, value)
                    .map_err(|err| HandleJsonResponseRefError::NumberSubtype(err.into()))?;
            }
            (DataSchemaSubtype::Integer(data_schema), Value::Number(response)) => {
                let value = get_integral_from_number(response).ok_or_else(|| {
                    HandleJsonResponseRefError::IntegerSubtype(NumericSubtypeError::InvalidType(
                        response.clone(),
                    ))
                })?;

                check_integer(data_schema, value)
                    .map_err(|err| HandleJsonResponseRefError::IntegerSubtype(err.into()))?;
            }

            (DataSchemaSubtype::String(data_schema), Value::String(response)) => {
                check_string(data_schema, response)
                    .map_err(|err| HandleJsonResponseRefError::StringSubtype(err.into()))?;
            }

            (DataSchemaSubtype::Object(data_schema), Value::Object(response)) => {
                if let Some(required) = &data_schema.required {
                    if required
                        .iter()
                        .all(|required| response.contains_key(required))
                        .not()
                    {
                        return Err(HandleJsonResponseRefError::ObjectSubtype(
                            ObjectSubtypeRefError::MissingRequired {
                                required,
                                data: response,
                            },
                        ));
                    }
                }

                if let Some(properties) = &data_schema.properties {
                    queue.extend(
                        response
                            .iter()
                            .filter_map(|(name, response)| {
                                properties.get(name).map(|property| (response, property))
                            })
                            .map(|(response, property)| ResponseValidatorState {
                                responses: json::ValueRef::from(response).to_refs(),
                                branching: ResponseValidatorBranching::Unchecked(property),
                                logic: ResponseValidatorLogic::And { parent },
                            }),
                    );
                }
            }

            _ => {
                return Err(HandleJsonResponseRefError::Subtype {
                    expected: subtype.into(),
                    found: response.0,
                })
            }
        }
    }

    // one_of needs to be handled upstream
    Ok(())
}

#[derive(Debug)]
pub enum HandleJsonResponseError {
    Json(reqwest::Error),
    WriteOnly,
    Constant {
        expected: serde_json::Value,
        found: serde_json::Value,
    },
    Enumeration {
        expected: Vec<serde_json::Value>,
        found: serde_json::Value,
    },
    OneOf {
        expected: Vec<DataSchema<(), (), ()>>,
        found: serde_json::Value,
    },
    Subtype {
        expected: DataSchemaStatelessSubtype,
        found: serde_json::Value,
    },
    ArraySubtype(ArraySubtypeError),
    NumberSubtype(NumericSubtypeError<f64, f64>),
    IntegerSubtype(NumericSubtypeError<i64, NonZeroU64>),
    StringSubtype(StringSubtypeError),
    ObjectSubtype(ObjectSubtypeError),
    Deserialization(serde_json::Error),
}

#[derive(Debug)]
pub enum HandleJsonResponseRefError<'a> {
    WriteOnly,
    Constant {
        expected: &'a serde_json::Value,
        found: &'a serde_json::Value,
    },
    Enumeration {
        expected: &'a [serde_json::Value],
        found: &'a serde_json::Value,
    },
    OneOf {
        expected: &'a [DataSchema<(), (), ()>],
        found: &'a serde_json::Value,
    },
    Subtype {
        expected: DataSchemaStatelessSubtype,
        found: &'a serde_json::Value,
    },
    ArraySubtype(ArraySubtypeError),
    NumberSubtype(NumericSubtypeError<f64, f64>),
    IntegerSubtype(NumericSubtypeError<i64, NonZeroU64>),
    StringSubtype(StringSubtypeRefError<'a>),
    ObjectSubtype(ObjectSubtypeRefError<'a>),
}

impl HandleJsonResponseRefError<'_> {
    pub fn into_owned(self) -> HandleJsonResponseError {
        match self {
            Self::WriteOnly => HandleJsonResponseError::WriteOnly,
            Self::Constant { expected, found } => HandleJsonResponseError::Constant {
                expected: expected.clone(),
                found: found.clone(),
            },
            Self::Enumeration { expected, found } => HandleJsonResponseError::Enumeration {
                expected: expected.to_vec(),
                found: found.clone(),
            },
            Self::OneOf { expected, found } => HandleJsonResponseError::OneOf {
                expected: expected.to_vec(),
                found: found.clone(),
            },
            Self::Subtype { expected, found } => HandleJsonResponseError::Subtype {
                expected,
                found: found.clone(),
            },
            Self::ArraySubtype(err) => HandleJsonResponseError::ArraySubtype(err),
            Self::NumberSubtype(err) => HandleJsonResponseError::NumberSubtype(err),
            Self::IntegerSubtype(err) => HandleJsonResponseError::IntegerSubtype(err),
            Self::StringSubtype(err) => HandleJsonResponseError::StringSubtype(err.into()),
            Self::ObjectSubtype(err) => HandleJsonResponseError::ObjectSubtype(err.into()),
        }
    }
}

#[derive(Debug)]
pub enum ArraySubtypeError {
    MinItems { expected: u32, found: usize },
    MaxItems { expected: u32, found: usize },
}

#[derive(Debug)]
pub enum NumericSubtypeError<T, U> {
    InvalidType(serde_json::Number),
    Minimum { expected: Minimum<T>, found: T },
    Maximum { expected: Maximum<T>, found: T },
    MultipleOf { expected: U, found: T },
}

impl<T, U> From<CheckNumericError<T, U>> for NumericSubtypeError<T, U> {
    fn from(value: CheckNumericError<T, U>) -> Self {
        match value {
            CheckNumericError::Minimum { expected, found } => Self::Minimum { expected, found },
            CheckNumericError::Maximum { expected, found } => Self::Maximum { expected, found },
            CheckNumericError::MultipleOf { expected, found } => {
                Self::MultipleOf { expected, found }
            }
        }
    }
}

#[derive(Debug)]
pub enum StringSubtypeRefError<'a> {
    MinLength { expected: u32, actual: usize },
    MaxLength { expected: u32, actual: usize },
    Pattern { pattern: &'a str, string: &'a str },
}

impl<'a> From<CheckStringError<'a>> for StringSubtypeRefError<'a> {
    fn from(value: CheckStringError<'a>) -> Self {
        match value {
            CheckStringError::MinLength { expected, actual } => {
                Self::MinLength { expected, actual }
            }
            CheckStringError::MaxLength { expected, actual } => {
                Self::MaxLength { expected, actual }
            }
            CheckStringError::Pattern { pattern, string } => Self::Pattern { pattern, string },
        }
    }
}

#[derive(Debug)]
pub enum StringSubtypeError {
    MinLength { expected: u32, actual: usize },
    MaxLength { expected: u32, actual: usize },
    Pattern { pattern: String, string: String },
}

impl From<StringSubtypeRefError<'_>> for StringSubtypeError {
    fn from(value: StringSubtypeRefError<'_>) -> Self {
        match value {
            StringSubtypeRefError::MinLength { expected, actual } => {
                Self::MinLength { expected, actual }
            }
            StringSubtypeRefError::MaxLength { expected, actual } => {
                Self::MaxLength { expected, actual }
            }
            StringSubtypeRefError::Pattern { pattern, string } => Self::Pattern {
                pattern: pattern.to_owned(),
                string: string.to_owned(),
            },
        }
    }
}

#[derive(Debug)]
pub enum ObjectSubtypeRefError<'a> {
    MissingRequired {
        required: &'a [String],
        data: &'a serde_json::Map<String, serde_json::Value>,
    },
}

#[derive(Debug)]
pub enum ObjectSubtypeError {
    MissingRequired(Vec<String>),
}

impl From<ObjectSubtypeRefError<'_>> for ObjectSubtypeError {
    fn from(value: ObjectSubtypeRefError<'_>) -> Self {
        match value {
            ObjectSubtypeRefError::MissingRequired { required, data } => {
                let missing = required
                    .iter()
                    .filter(|&required| data.contains_key(required).not())
                    .cloned()
                    .collect();
                Self::MissingRequired(missing)
            }
        }
    }
}

fn validate_uri_variable_value(
    schema: &ScalarDataSchema,
    value: &UriVariableValue,
) -> Result<(), InvalidUriVariableKind> {
    if schema.read_only {
        return Err(InvalidUriVariableKind::ReadOnly);
    }

    if let Some(constant) = &schema.constant {
        if value != constant {
            return Err(InvalidUriVariableKind::Constant(constant.clone()));
        }
    }

    if let Some(enumeration) = &schema.enumeration {
        if enumeration.iter().any(|variant| value == variant).not() {
            return Err(InvalidUriVariableKind::Enumeration(enumeration.clone()));
        }
    }

    if let Some(subtype) = &schema.subtype {
        match subtype {
            ScalarDataSchemaSubtype::Boolean => {
                if matches!(value, UriVariableValue::Boolean(_)).not() {
                    return Err(InvalidUriVariableKind::Subtype(
                        InvalidUriVariableKindSubtype::Boolean,
                    ));
                }
            }
            ScalarDataSchemaSubtype::Number(schema) => {
                let &UriVariableValue::Number(value) = value else {
                    return Err(InvalidUriVariableKind::Subtype(
                        InvalidUriVariableKindSubtype::Number(
                            InvalidUriVariableKindSubtypeNumeric::Type
                        )
                    ));
                };

                check_number(schema, value).map_err(|err| {
                    InvalidUriVariableKind::Subtype(InvalidUriVariableKindSubtype::Number(
                        err.into(),
                    ))
                })?;
            }
            ScalarDataSchemaSubtype::Integer(schema) => {
                let value = match *value {
                    UriVariableValue::Integer(value) => value,
                    UriVariableValue::Number(value) if value.fract().abs() < f64::EPSILON => {
                        value as i64
                    }
                    _ => {
                        return Err(InvalidUriVariableKind::Subtype(
                            InvalidUriVariableKindSubtype::Integer(
                                InvalidUriVariableKindSubtypeNumeric::Type,
                            ),
                        ))
                    }
                };

                check_integer(schema, value).map_err(|err| {
                    InvalidUriVariableKind::Subtype(InvalidUriVariableKindSubtype::Integer(
                        err.into(),
                    ))
                })?;
            }
            ScalarDataSchemaSubtype::String(schema) => {
                let UriVariableValue::String(value) = value else {
                    return Err(InvalidUriVariableKind::Subtype(
                        InvalidUriVariableKindSubtype::String(
                            InvalidUriVariableKindSubtypeString::Type
                        )
                    ));
                };

                check_string(schema, value).map_err(|err| {
                    InvalidUriVariableKind::Subtype(InvalidUriVariableKindSubtype::String(
                        err.into(),
                    ))
                })?;
            }
            ScalarDataSchemaSubtype::Null => {
                // There is no possible way to universally represent a null uri variable.
                return Err(InvalidUriVariableKind::Subtype(
                    InvalidUriVariableKindSubtype::Null,
                ));
            }
        }
    }

    match &schema.one_of {
        Some(one_of) => {
            for schema in one_of {
                if validate_uri_variable_value(schema, value).is_ok() {
                    return Ok(());
                }
            }

            Err(InvalidUriVariableKind::OneOf)
        }
        None => Ok(()),
    }
}

fn is_above_minimum<T>(value: &T, minimum: &Minimum<T>) -> bool
where
    T: PartialOrd,
{
    match minimum {
        Minimum::Inclusive(minimum) => value >= minimum,
        Minimum::Exclusive(minimum) => value > minimum,
    }
}

fn is_below_maximum<T>(value: &T, maximum: &Maximum<T>) -> bool
where
    T: PartialOrd,
{
    match maximum {
        Maximum::Inclusive(maximum) => value <= maximum,
        Maximum::Exclusive(maximum) => value < maximum,
    }
}

#[derive(Debug, Clone)]
pub enum InvalidUriVariableKind {
    Constant(serde_json::Value),
    Enumeration(Vec<serde_json::Value>),
    Subtype(InvalidUriVariableKindSubtype),
    ReadOnly,
    OneOf,
}

#[derive(Debug, Clone)]
pub enum InvalidUriVariableKindSubtype {
    Boolean,
    Number(InvalidUriVariableKindSubtypeNumeric<f64, f64>),
    Integer(InvalidUriVariableKindSubtypeNumeric<i64, NonZeroU64>),
    String(InvalidUriVariableKindSubtypeString),
    Null,
}

#[derive(Debug, Clone)]
pub enum InvalidUriVariableKindSubtypeNumeric<T, U> {
    Type,
    BelowMinimum(Minimum<T>),
    AboveMaximum(Maximum<T>),
    NotMultipleOf(U),
}

impl<T, U> From<CheckNumericError<T, U>> for InvalidUriVariableKindSubtypeNumeric<T, U> {
    fn from(value: CheckNumericError<T, U>) -> Self {
        match value {
            CheckNumericError::Minimum { expected, .. } => Self::BelowMinimum(expected),
            CheckNumericError::Maximum { expected, .. } => Self::AboveMaximum(expected),
            CheckNumericError::MultipleOf { expected, .. } => Self::NotMultipleOf(expected),
        }
    }
}

#[derive(Debug, Clone)]
pub enum InvalidUriVariableKindSubtypeString {
    Type,
    MinLength { expected: u32, actual: usize },
    MaxLength { expected: u32, actual: usize },
    Pattern { pattern: String, string: String },
}

impl From<CheckStringError<'_>> for InvalidUriVariableKindSubtypeString {
    fn from(value: CheckStringError) -> Self {
        match value {
            CheckStringError::MinLength { expected, actual } => {
                Self::MinLength { expected, actual }
            }
            CheckStringError::MaxLength { expected, actual } => {
                Self::MaxLength { expected, actual }
            }
            CheckStringError::Pattern { pattern, string } => Self::Pattern {
                pattern: pattern.to_owned(),
                string: string.to_owned(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataSchemaStatelessSubtype {
    Array,
    Boolean,
    Number,
    Integer,
    Object,
    String,
    Null,
}

impl<DS, AS, OS> From<&DataSchemaSubtype<DS, AS, OS>> for DataSchemaStatelessSubtype {
    #[inline]
    fn from(value: &DataSchemaSubtype<DS, AS, OS>) -> Self {
        match value {
            DataSchemaSubtype::Array(_) => Self::Array,
            DataSchemaSubtype::Boolean => Self::Boolean,
            DataSchemaSubtype::Number(_) => Self::Number,
            DataSchemaSubtype::Integer(_) => Self::Integer,
            DataSchemaSubtype::Object(_) => Self::Object,
            DataSchemaSubtype::String(_) => Self::String,
            DataSchemaSubtype::Null => Self::Null,
        }
    }
}

impl From<&serde_json::Value> for DataSchemaStatelessSubtype {
    #[inline]
    fn from(value: &serde_json::Value) -> Self {
        use serde_json::Value;

        match value {
            Value::Array(_) => Self::Array,
            Value::Null => Self::Null,
            Value::Bool(_) => Self::Boolean,
            Value::Number(_) => Self::Number,
            Value::String(_) => Self::String,
            Value::Object(_) => Self::Object,
        }
    }
}

fn check_number(data_schema: &NumberSchema, value: f64) -> Result<(), CheckNumericError<f64, f64>> {
    if let Some(minimum) = data_schema.minimum {
        if is_above_minimum(&value, &minimum).not() {
            return Err(CheckNumericError::Minimum {
                expected: minimum,
                found: value,
            });
        }
    }

    if let Some(maximum) = data_schema.maximum {
        if is_below_maximum(&value, &maximum).not() {
            return Err(CheckNumericError::Maximum {
                expected: maximum,
                found: value,
            });
        }
    }

    if let Some(multiple_of) = data_schema.multiple_of {
        // We should **never** get `multiple_of` less or equal than 0, this condition
        // should be checked during the deserialization of the TD. If this prerequisite
        // is not satisfied, we just consider the multiplicative constraint not
        // satisfied.
        let is_multiple_of = multiple_of > 0. && f64::abs(value % multiple_of) < f64::EPSILON;

        if is_multiple_of.not() {
            return Err(CheckNumericError::MultipleOf {
                expected: multiple_of,
                found: value,
            });
        }
    }

    Ok(())
}

fn check_integer(
    data_schema: &IntegerSchema,
    value: i64,
) -> Result<(), CheckNumericError<i64, NonZeroU64>> {
    if let Some(minimum) = data_schema.minimum {
        if is_above_minimum(&value, &minimum).not() {
            return Err(CheckNumericError::Minimum {
                expected: minimum,
                found: value,
            });
        }
    }

    if let Some(maximum) = data_schema.maximum {
        if is_below_maximum(&value, &maximum).not() {
            return Err(CheckNumericError::Maximum {
                expected: maximum,
                found: value,
            });
        }
    }

    if let Some(multiple_of) = data_schema.multiple_of {
        // Reasoning: if the value of `multiply_of` is above i64::MAX, the value cannot
        // be a multiple of that. Otherwise we check the modulo, and because we only
        // need to check whether it is zero or not, it does not matter if we use the
        // truncated (%) or the euclidean form.
        let is_multiple_of = i64::try_from(multiple_of.get())
            .ok()
            .map_or(false, |multiple_of| value % multiple_of == 0);

        if is_multiple_of.not() {
            return Err(CheckNumericError::MultipleOf {
                expected: multiple_of,
                found: value,
            });
        }
    }

    Ok(())
}

#[derive(Debug)]
enum CheckNumericError<T, U> {
    Minimum { expected: Minimum<T>, found: T },
    Maximum { expected: Maximum<T>, found: T },
    MultipleOf { expected: U, found: T },
}

fn check_string<'a>(
    data_schema: &'a StringSchema,
    value: &'a str,
) -> Result<(), CheckStringError<'a>> {
    if data_schema.content_encoding.is_some() {
        warn!("content encoding is still not supported");
    }

    if data_schema.min_length.is_some() || data_schema.max_length.is_some() {
        // Quote from the WoT-TD 1.1 spec:
        //
        // The length of a string (i.e., minLength and maxLength) is defined as the
        // number of Unicode code points, as defined by RFC8259.
        let code_points = value.chars().count();
        let code_points_u32 = u32::try_from(code_points);

        if let Some(min_length) = data_schema.min_length {
            if code_points_u32.map_or(true, |code_points| code_points < min_length) {
                return Err(CheckStringError::MinLength {
                    expected: min_length,
                    actual: code_points,
                });
            }
        }

        if let Some(max_length) = data_schema.max_length {
            if code_points_u32.map_or(true, |code_points| code_points > max_length) {
                return Err(CheckStringError::MaxLength {
                    expected: max_length,
                    actual: code_points,
                });
            }
        }
    }

    if let Some(pattern) = &data_schema.pattern {
        let is_valid_pattern = Regex::new(pattern).map_or(false, |regex| regex.is_match(value));
        if is_valid_pattern.not() {
            return Err(CheckStringError::Pattern {
                pattern,
                string: value,
            });
        }
    }

    Ok(())
}

#[derive(Debug)]
enum CheckStringError<'a> {
    MinLength { expected: u32, actual: usize },
    MaxLength { expected: u32, actual: usize },
    Pattern { pattern: &'a str, string: &'a str },
}

fn get_integral_from_number(number: &serde_json::Number) -> Option<i64> {
    if let Some(value) = number.as_u64() {
        value.try_into().ok()
    } else if let Some(value) = number.as_i64() {
        Some(value)
    } else if let Some(value) = number.as_f64() {
        (value.fract().abs() < f64::EPSILON).then_some(value as i64)
    } else {
        None
    }
}

#[derive(Debug)]
pub struct PropertyWriter<'a, T> {
    status: PropertyReaderWriterStatus<'a>,
    value: &'a T,
}

impl<'a, T> PropertyWriter<'a, T> {
    #[inline]
    pub fn no_security(&mut self) -> &mut Self {
        self.status.no_security();
        self
    }

    #[inline]
    pub fn basic(&mut self, username: impl Into<String>, password: impl Into<String>) -> &mut Self {
        self.status.basic(username, password);
        self
    }

    #[inline]
    pub fn protocol(&mut self, accepted_protocol: Protocol) -> &mut Self {
        self.status.protocol(accepted_protocol);
        self
    }

    #[inline]
    pub fn protocols(&mut self, accepted_protocols: &[Protocol]) -> &mut Self {
        self.status.protocols(accepted_protocols);
        self
    }

    #[inline]
    pub fn bearer(&mut self, token: impl Into<String>) -> &mut Self {
        self.status.bearer(token);
        self
    }

    #[inline]
    pub fn uri_variable<N>(&mut self, name: N, value: UriVariableValue) -> &mut Self
    where
        N: Into<String> + AsRef<str>,
    {
        self.status.uri_variable(name, value);
        self
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::json::json_ref;

    use super::*;

    #[test]
    fn validate_integer() {
        handle_json_response_validate(
            json_ref!(42),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Integer(IntegerSchema::default())),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(42),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Integer(IntegerSchema {
                    maximum: Some(Maximum::Inclusive(42)),
                    minimum: Some(Minimum::Inclusive(42)),
                    multiple_of: Some(7.try_into().unwrap()),
                })),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(42),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Integer(IntegerSchema {
                    maximum: Some(Maximum::Exclusive(43)),
                    minimum: Some(Minimum::Exclusive(41)),
                    multiple_of: None,
                })),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(42),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Integer(IntegerSchema {
                        minimum: Some(Minimum::Inclusive(43)),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::IntegerSubtype(NumericSubtypeError::Minimum {
                expected: Minimum::Inclusive(43),
                found: 42
            })
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(42),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Integer(IntegerSchema {
                        minimum: Some(Minimum::Exclusive(42)),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::IntegerSubtype(NumericSubtypeError::Minimum {
                expected: Minimum::Exclusive(42),
                found: 42
            })
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(42),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Integer(IntegerSchema {
                        maximum: Some(Maximum::Inclusive(41)),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::IntegerSubtype(NumericSubtypeError::Maximum {
                expected: Maximum::Inclusive(41),
                found: 42
            })
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(42),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Integer(IntegerSchema {
                        maximum: Some(Maximum::Exclusive(42)),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::IntegerSubtype(NumericSubtypeError::Maximum {
                expected: Maximum::Exclusive(42),
                found: 42
            })
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(42),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Integer(IntegerSchema {
                        multiple_of: Some(5.try_into().unwrap()),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::IntegerSubtype(NumericSubtypeError::MultipleOf {
                expected,
                found: 42
            })
            if expected.get() == 5
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!("hello"),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Integer(IntegerSchema::default())),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::Subtype {
                expected: DataSchemaStatelessSubtype::Integer,
                found: serde_json::Value::String(found)
            }
            if found == "hello"
        ));

        handle_json_response_validate(
            json_ref!(42.),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Integer(IntegerSchema::default())),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(42.5),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Integer(IntegerSchema::default())),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::IntegerSubtype(NumericSubtypeError::InvalidType(invalid))
            if invalid == serde_json::Number::from_f64(42.5).unwrap()
        ));
    }

    #[test]
    fn validate_number() {
        handle_json_response_validate(
            json_ref!(42.5),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Number(NumberSchema::default())),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(42.5),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Number(NumberSchema {
                    maximum: Some(Maximum::Inclusive(42.5)),
                    minimum: Some(Minimum::Inclusive(42.5)),
                    multiple_of: Some(0.5),
                })),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(42.5),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Number(NumberSchema {
                    maximum: Some(Maximum::Exclusive(42.6)),
                    minimum: Some(Minimum::Exclusive(42.4)),
                    multiple_of: None,
                })),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(42.5),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Number(NumberSchema {
                        minimum: Some(Minimum::Inclusive(42.6)),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::NumberSubtype(NumericSubtypeError::Minimum {
                expected: Minimum::Inclusive(expected),
                found,
            })
            if expected == 42.6 && found == 42.5
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(42.5),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Number(NumberSchema {
                        minimum: Some(Minimum::Exclusive(42.5)),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::NumberSubtype(NumericSubtypeError::Minimum {
                expected: Minimum::Exclusive(expected),
                found,
            })
            if expected == 42.5 && found == 42.5
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(42.5),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Number(NumberSchema {
                        maximum: Some(Maximum::Inclusive(42.4)),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::NumberSubtype(NumericSubtypeError::Maximum {
                expected: Maximum::Inclusive(expected),
                found,
            })
            if expected == 42.4 && found == 42.5
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(42.5),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Number(NumberSchema {
                        maximum: Some(Maximum::Exclusive(42.5)),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::NumberSubtype(NumericSubtypeError::Maximum {
                expected: Maximum::Exclusive(expected),
                found,
            })
            if expected == 42.5 && found == 42.5
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(42.5),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Number(NumberSchema {
                        multiple_of: Some(0.7),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::NumberSubtype(NumericSubtypeError::MultipleOf {
                expected,
                found,
            })
            if (expected - 0.7).abs() < f64::EPSILON && found == 42.5
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!("hello"),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Number(NumberSchema::default())),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::Subtype {
                expected: DataSchemaStatelessSubtype::Number,
                found: serde_json::Value::String(found)
            }
            if found == "hello"
        ));

        handle_json_response_validate(
            json_ref!(42),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Number(NumberSchema::default())),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json::ValueRef(&serde_json::Value::Number(serde_json::Number::from(42i64))),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Number(NumberSchema::default())),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(-42),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Number(NumberSchema::default())),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(i64::MAX),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Number(NumberSchema::default())),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::NumberSubtype(NumericSubtypeError::InvalidType(invalid))
            if invalid == serde_json::Number::from(i64::MAX)
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(u64::MAX),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Number(NumberSchema::default())),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::NumberSubtype(NumericSubtypeError::InvalidType(invalid))
            if invalid == serde_json::Number::from(u64::MAX)
        ));
    }

    #[test]
    fn validate_string() {
        handle_json_response_validate(
            json_ref!("hello 👋🏼"),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::String(StringSchema::default())),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!("hello 👋🏼"),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::String(StringSchema {
                    min_length: Some(8),
                    max_length: Some(8),
                    pattern: Some(r#"^[Hh]el{1,2}o(?: 👋[🏻🏼🏽🏾🏿]?)?( world)?!?"#.to_string()),
                    content_encoding: Some("blorgz".to_string()),
                    content_media_type: Some("plain/text".to_string()),
                })),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!("hello 👋🏼"),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::String(StringSchema {
                        min_length: Some(9),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::StringSubtype(StringSubtypeRefError::MinLength {
                expected: 9,
                actual: 8,
            })
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!("hello 👋🏼"),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::String(StringSchema {
                        max_length: Some(7),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::StringSubtype(StringSubtypeRefError::MaxLength {
                expected: 7,
                actual: 8,
            })
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!("hello 👋🏼"),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::String(StringSchema {
                        pattern: Some(r#"^hello( world)?$"#.to_string()),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::StringSubtype(StringSubtypeRefError::Pattern {
                pattern: r#"^hello( world)?$"#,
                string: "hello 👋🏼",
            })
        ));
    }

    #[test]
    fn validate_null() {
        handle_json_response_validate(
            json_ref!(null),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Null),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(3),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Null),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::Subtype {
                expected: DataSchemaStatelessSubtype::Null,
                found: serde_json::Value::Number(found),
            }
            if found.as_u64() == Some(3)
        ))
    }

    #[test]
    fn validate_boolean() {
        handle_json_response_validate(
            json_ref!(true),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Boolean),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(false),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Boolean),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(3),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Boolean),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::Subtype {
                expected: DataSchemaStatelessSubtype::Boolean,
                found: serde_json::Value::Number(found),
            }
            if found.as_u64() == Some(3)
        ))
    }

    #[test]
    fn validate_array() {
        handle_json_response_validate(
            json_ref!([1, "two", 3., null, true, ["nes", "ted"], {"hello": 1, "world": true}]),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Array(ArraySchema::default())),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!([1, "two", 3., null, true, ["nes", "ted"], {"hello": 1, "world": true}]),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Array(ArraySchema {
                    items: Some(BoxedElemOrVec::Vec(vec![
                        DataSchema {
                            subtype: Some(DataSchemaSubtype::Integer(IntegerSchema::default())),
                            ..Default::default()
                        },
                        DataSchema {
                            subtype: Some(DataSchemaSubtype::String(StringSchema::default())),
                            ..Default::default()
                        },
                        DataSchema {
                            subtype: Some(DataSchemaSubtype::Number(NumberSchema::default())),
                            ..Default::default()
                        },
                        DataSchema {
                            subtype: Some(DataSchemaSubtype::Null),
                            ..Default::default()
                        },
                        DataSchema {
                            subtype: Some(DataSchemaSubtype::Boolean),
                            ..Default::default()
                        },
                        DataSchema {
                            subtype: Some(DataSchemaSubtype::Array(ArraySchema::default())),
                            ..Default::default()
                        },
                        DataSchema {
                            subtype: Some(DataSchemaSubtype::Object(ObjectSchema::default())),
                            ..Default::default()
                        },
                    ])),
                    min_items: Some(7),
                    max_items: Some(7),
                    other: (),
                })),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!([1, 2, 3, 4, 5, 6, 7]),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Array(ArraySchema {
                    items: Some(BoxedElemOrVec::Elem(Box::new(DataSchema {
                        subtype: Some(DataSchemaSubtype::Integer(IntegerSchema::default())),
                        ..Default::default()
                    }))),
                    min_items: Some(7),
                    max_items: Some(7),
                    other: (),
                })),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!([1, "two", 3., null, true, ["nes", "ted"], {"hello": 1, "world": true}]),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Array(ArraySchema {
                    items: Some(BoxedElemOrVec::Vec(vec![DataSchema {
                        subtype: Some(DataSchemaSubtype::Integer(IntegerSchema::default())),
                        ..Default::default()
                    }])),
                    min_items: Some(7),
                    max_items: Some(7),
                    other: (),
                })),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!([1, "two", null, true, ["nes", "ted"], {"hello": 1, "world": true}]),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Array(ArraySchema {
                        items: Some(BoxedElemOrVec::Vec(vec![
                            DataSchema {
                                subtype: Some(DataSchemaSubtype::Integer(IntegerSchema::default())),
                                ..Default::default()
                            },
                            DataSchema {
                                subtype: Some(DataSchemaSubtype::String(StringSchema::default())),
                                ..Default::default()
                            },
                            DataSchema {
                                subtype: Some(DataSchemaSubtype::Number(NumberSchema::default())),
                                ..Default::default()
                            },
                        ])),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::Subtype {
                expected: DataSchemaStatelessSubtype::Number,
                found: &serde_json::Value::Null,
            }
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!([1, 2, 3., 3.5, 4., 5]),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Array(ArraySchema {
                        items: Some(BoxedElemOrVec::Elem(Box::new(DataSchema {
                            subtype: Some(DataSchemaSubtype::Integer(IntegerSchema::default())),
                            ..Default::default()
                        }))),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::IntegerSubtype(NumericSubtypeError::InvalidType(found))
            if found.as_f64() == Some(3.5)
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!([1, "two", 3., null, true, ["nes", "ted"], {"hello": 1, "world": true}]),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Array(ArraySchema {
                        min_items: Some(8),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::ArraySubtype(ArraySubtypeError::MinItems {
                expected: 8,
                found: 7,
            })
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!([1, "two", 3., null, true, ["nes", "ted"], {"hello": 1, "world": true}]),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Array(ArraySchema {
                        max_items: Some(6),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::ArraySubtype(ArraySubtypeError::MaxItems {
                expected: 6,
                found: 7,
            })
        ));
    }

    #[test]
    fn validate_object() {
        handle_json_response_validate(
            json_ref!({
                "hello": null,
                "world": 3.5,
                "this": "is",
                "fine": true,
                "house": ["on", {
                    "fire": 234,
                    "water": 1,
                }],
            }),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Object(ObjectSchema::default())),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!({
                "hello": null,
                "world": 3.5,
                "this": "is",
                "fine": true,
                "house": ["on", {
                    "fire": 234,
                    "water": 1,
                }],
            }),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Object(ObjectSchema {
                    properties: Some(
                        [
                            (
                                "hello".to_string(),
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::Null),
                                    ..Default::default()
                                },
                            ),
                            (
                                "world".to_string(),
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::Number(
                                        NumberSchema::default(),
                                    )),
                                    ..Default::default()
                                },
                            ),
                            (
                                "this".to_string(),
                                DataSchema {
                                    constant: Some(json!("is")),
                                    ..Default::default()
                                },
                            ),
                            (
                                "fine".to_string(),
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::Boolean),
                                    ..Default::default()
                                },
                            ),
                            (
                                "house".to_string(),
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::Array(ArraySchema::default())),
                                    ..Default::default()
                                },
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    required: Some(vec![
                        "hello".to_string(),
                        "world".to_string(),
                        "this".to_string(),
                        "fine".to_string(),
                    ]),
                    other: (),
                })),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!({
                "hello": null,
            }),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Object(ObjectSchema {
                    properties: Some(
                        [
                            (
                                "hello".to_string(),
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::Null),
                                    ..Default::default()
                                },
                            ),
                            (
                                "world".to_string(),
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::Number(
                                        NumberSchema::default(),
                                    )),
                                    ..Default::default()
                                },
                            ),
                            (
                                "this".to_string(),
                                DataSchema {
                                    constant: Some(json!("is")),
                                    ..Default::default()
                                },
                            ),
                            (
                                "fine".to_string(),
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::Boolean),
                                    ..Default::default()
                                },
                            ),
                            (
                                "house".to_string(),
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::Array(ArraySchema::default())),
                                    ..Default::default()
                                },
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    required: Some(vec!["hello".to_string()]),
                    other: (),
                })),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!({
                "hello": null,
                "world": 3.5,
                "this": "is",
                "fine": true,
                "house": ["on", {
                    "fire": 234,
                    "water": 1,
                }],
            }),
            &DataSchema {
                subtype: Some(DataSchemaSubtype::Object(ObjectSchema::default())),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!({
                    "hello": null,
                    "world": 3.5,
                    "this": "is",
                    "fine": 234,
                    "house": ["on", {
                        "fire": 234,
                        "water": 1,
                    }],
                }),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Object(ObjectSchema {
                        properties: Some(
                            [
                                (
                                    "hello".to_string(),
                                    DataSchema {
                                        subtype: Some(DataSchemaSubtype::Null),
                                        ..Default::default()
                                    },
                                ),
                                (
                                    "world".to_string(),
                                    DataSchema {
                                        subtype: Some(DataSchemaSubtype::Number(
                                            NumberSchema::default(),
                                        )),
                                        ..Default::default()
                                    },
                                ),
                                (
                                    "this".to_string(),
                                    DataSchema {
                                        constant: Some(json!("is")),
                                        ..Default::default()
                                    },
                                ),
                                (
                                    "fine".to_string(),
                                    DataSchema {
                                        subtype: Some(DataSchemaSubtype::Boolean),
                                        ..Default::default()
                                    },
                                ),
                                (
                                    "house".to_string(),
                                    DataSchema {
                                        subtype: Some(DataSchemaSubtype::Array(
                                            ArraySchema::default()
                                        )),
                                        ..Default::default()
                                    },
                                ),
                            ]
                            .into_iter()
                            .collect(),
                        ),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::Subtype {
                expected: DataSchemaStatelessSubtype::Boolean,
                found: serde_json::Value::Number(found)
            }
            if found.as_u64() == Some(234)
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!({
                    "hello": null,
                    "world": 3.5,
                    "fine": true,
                    "house": ["on", {
                        "fire": true,
                        "water": 1,
                    }],
                }),
                &DataSchema {
                    subtype: Some(DataSchemaSubtype::Object(ObjectSchema {
                        required: Some(vec![
                            "hello".to_string(),
                            "world".to_string(),
                            "this".to_string(),
                            "fine".to_string()
                        ]),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::ObjectSubtype (
                ObjectSubtypeRefError::MissingRequired { required, data }
            )
            if
                required.iter().eq(["hello", "world", "this", "fine"]) &&
                {
                    let mut data: Vec<_> = data.keys().collect();
                    data.sort_unstable();
                    data == ["fine", "hello", "house", "world"]
                }
        ));
    }

    #[test]
    fn validate_constant() {
        handle_json_response_validate(
            json_ref!({
                "hello": null,
                "world": 3.5,
                "this": "is",
                "fine": true,
            }),
            &DataSchema {
                constant: Some(json!({
                    "hello": null,
                    "world": 3.5,
                    "this": "is",
                    "fine": true,
                })),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(3),
            &DataSchema {
                constant: Some(json!(3.)),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(3.),
            &DataSchema {
                constant: Some(json!(3)),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(3),
            &DataSchema {
                constant: Some(json!(3)),
                subtype: Some(DataSchemaSubtype::Integer(IntegerSchema {
                    maximum: Some(Maximum::Inclusive(3)),
                    minimum: Some(Minimum::Inclusive(3)),
                    multiple_of: Some(3.try_into().unwrap()),
                })),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(3),
                &DataSchema {
                    constant: Some(json!(4)),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::Constant { expected, found }
            if
                expected.as_u64() == Some(4) &&
                found.as_u64() == Some(3)
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(3),
                &DataSchema {
                    constant: Some(json!(3)),
                    subtype: Some(DataSchemaSubtype::String(StringSchema::default())),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::Subtype {
                expected: DataSchemaStatelessSubtype::String,
                found: serde_json::Value::Number(found),
            }
            if found.as_u64() == Some(3)
        ));
    }

    #[test]
    fn validate_enumeration() {
        handle_json_response_validate(
            json_ref!("hello"),
            &DataSchema {
                enumeration: Some(vec!["hello".into(), 3.5.into(), 5.into(), true.into()]),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(3.5),
            &DataSchema {
                enumeration: Some(vec!["hello".into(), 3.5.into(), 5.into(), true.into()]),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(5),
            &DataSchema {
                enumeration: Some(vec!["hello".into(), 3.5.into(), 5.into(), true.into()]),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(true),
            &DataSchema {
                enumeration: Some(vec!["hello".into(), 3.5.into(), 5.into(), true.into()]),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(5.0),
            &DataSchema {
                enumeration: Some(vec!["hello".into(), 3.5.into(), 5.into(), true.into()]),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(5.0),
            &DataSchema {
                enumeration: Some(vec!["hello".into(), 3.5.into(), 5.into(), true.into()]),
                subtype: Some(DataSchemaSubtype::Integer(IntegerSchema {
                    maximum: Some(Maximum::Inclusive(5)),
                    minimum: Some(Minimum::Inclusive(5)),
                    multiple_of: Some(5.try_into().unwrap()),
                })),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(3),
                &DataSchema {
                    enumeration: Some(vec!["hello".into(), 3.5.into(), 5.into(), true.into()]),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::Enumeration { expected, found }
            if
                expected.iter().eq(&[json!("hello"), json!(3.5), json!(5), json!(true)]) &&
                found.as_u64() == Some(3)
        ));

        assert!(matches!(
           handle_json_response_validate(
               json_ref!(3.5),
               &DataSchema {
                   enumeration: Some(vec!["hello".into(), 3.5.into(), 5.into(), true.into()]),
                   subtype: Some(DataSchemaSubtype::Integer(IntegerSchema::default())),
                   ..Default::default()
               },
           )
           .unwrap_err(),
           HandleJsonResponseRefError::IntegerSubtype (
               NumericSubtypeError::InvalidType(found)
           )
           if found.as_f64() == Some(3.5)
        ));
    }

    #[test]
    fn validate_one_of() {
        handle_json_response_validate(
            json_ref!("hello"),
            &DataSchema {
                one_of: Some(vec![
                    DataSchema {
                        constant: Some(json!("hello")),
                        ..Default::default()
                    },
                    DataSchema {
                        constant: Some(json!("world")),
                        ..Default::default()
                    },
                    DataSchema {
                        constant: Some(json!(3)),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!("world"),
            &DataSchema {
                one_of: Some(vec![
                    DataSchema {
                        constant: Some(json!("hello")),
                        ..Default::default()
                    },
                    DataSchema {
                        constant: Some(json!("world")),
                        ..Default::default()
                    },
                    DataSchema {
                        constant: Some(json!(3)),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            },
        )
        .unwrap();

        handle_json_response_validate(
            json_ref!(3),
            &DataSchema {
                one_of: Some(vec![
                    DataSchema {
                        constant: Some(json!("hello")),
                        ..Default::default()
                    },
                    DataSchema {
                        constant: Some(json!("world")),
                        ..Default::default()
                    },
                    DataSchema {
                        constant: Some(json!(3)),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!("nope"),
                &DataSchema {
                    one_of: Some(vec![
                        DataSchema {
                            constant: Some(json!("hello")),
                            ..Default::default()
                        },
                        DataSchema {
                            constant: Some(json!("world")),
                            ..Default::default()
                        },
                        DataSchema {
                            constant: Some(json!(3)),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::OneOf {
                expected: [
                    DataSchema {
                        constant: Some(serde_json::Value::String(const1)),
                        ..
                    },
                    DataSchema {
                        constant: Some(serde_json::Value::String(const2)),
                        ..
                    },
                    DataSchema {
                        constant: Some(serde_json::Value::Number(number)),
                        ..
                    },
                ],
                found: serde_json::Value::String(found),
            }
            if const1 == "hello"
            && const2 == "world"
            && number.as_u64() == Some(3)
            && found == "nope"
        ));

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(3),
                &DataSchema {
                    one_of: Some(vec![]),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::OneOf {
                expected: [],
                found: serde_json::Value::Number(found),
            }
            if found.as_u64() == Some(3)
        ));

        handle_json_response_validate(
            json_ref!(3),
            &DataSchema {
                one_of: Some(vec![DataSchema {
                    constant: Some(json!(3)),
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(matches!(
            handle_json_response_validate(
                json_ref!(3),
                &DataSchema {
                    one_of: Some(vec![DataSchema {
                        constant: Some(json!("hello")),
                        ..Default::default()
                    }]),
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::OneOf {
                expected: [DataSchema {
                    constant: Some(serde_json::Value::String(expected)),
                    ..
                }],
                found: serde_json::Value::Number(found),
            }
            if expected == "hello"
            && found.as_u64() == Some(3)
        ));

        let chad_schema = DataSchema {
            one_of: Some(vec![
                DataSchema {
                    subtype: Some(DataSchemaSubtype::Array(ArraySchema {
                        items: Some(BoxedElemOrVec::Elem(Box::new(DataSchema {
                            one_of: Some(vec![
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::Integer(
                                        IntegerSchema::default(),
                                    )),
                                    ..Default::default()
                                },
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::String(
                                        StringSchema::default(),
                                    )),
                                    ..Default::default()
                                },
                            ]),
                            ..Default::default()
                        }))),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                DataSchema {
                    subtype: Some(DataSchemaSubtype::Array(ArraySchema {
                        items: Some(BoxedElemOrVec::Elem(Box::new(DataSchema {
                            one_of: Some(vec![
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::Integer(
                                        IntegerSchema::default(),
                                    )),
                                    ..Default::default()
                                },
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::Number(
                                        NumberSchema::default(),
                                    )),
                                    ..Default::default()
                                },
                            ]),
                            ..Default::default()
                        }))),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                DataSchema {
                    subtype: Some(DataSchemaSubtype::Array(ArraySchema {
                        items: Some(BoxedElemOrVec::Elem(Box::new(DataSchema {
                            one_of: Some(vec![
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::Number(NumberSchema {
                                        minimum: Some(Minimum::Inclusive(3.)),
                                        ..Default::default()
                                    })),
                                    ..Default::default()
                                },
                                DataSchema {
                                    subtype: Some(DataSchemaSubtype::String(
                                        StringSchema::default(),
                                    )),
                                    ..Default::default()
                                },
                            ]),
                            ..Default::default()
                        }))),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };
        handle_json_response_validate(json_ref!([3]), &chad_schema).unwrap();
        handle_json_response_validate(json_ref!([3, 3.5]), &chad_schema).unwrap();
        handle_json_response_validate(json_ref!([3, "pi"]), &chad_schema).unwrap();
        handle_json_response_validate(json_ref!(["pi", 3.5]), &chad_schema).unwrap();
        handle_json_response_validate(json_ref!(["pi", 3.5, 3]), &chad_schema).unwrap();
        assert!(matches!(
            handle_json_response_validate(json_ref!(["pi", 3.5, 2]), &chad_schema).unwrap_err(),
            HandleJsonResponseRefError::OneOf { .. },
        ));
    }

    #[test]
    fn read_write_only_properties() {
        assert!(matches!(
            handle_json_response_validate(
                json_ref!(3),
                &DataSchema {
                    write_only: true,
                    ..Default::default()
                },
            )
            .unwrap_err(),
            HandleJsonResponseRefError::WriteOnly,
        ));

        handle_json_response_validate(
            json_ref!(3),
            &DataSchema {
                one_of: Some(vec![
                    DataSchema {
                        write_only: true,
                        ..Default::default()
                    },
                    DataSchema::default(),
                ]),
                ..Default::default()
            },
        )
        .unwrap();
    }

    #[test]
    fn scalar_data_schema_from_data_schema() {
        let scalar = ScalarDataSchema::try_from(&DataSchema::<(), (), ()> {
            attype: Some(vec!["attype1".to_string(), "attype2".to_string()]),
            title: Some("title".to_string()),
            titles: Some(
                [
                    ("it".parse().unwrap(), "un titolo".to_string()),
                    ("en".parse().unwrap(), "a title".to_string()),
                ]
                .into_iter()
                .collect(),
            ),
            description: Some("description".to_string()),
            descriptions: Some(
                [
                    ("it".parse().unwrap(), "una descrizione".to_string()),
                    ("en".parse().unwrap(), "a description".to_string()),
                ]
                .into_iter()
                .collect(),
            ),
            constant: Some("const".into()),
            default: Some(json!({"default": ["value", 3]})),
            unit: Some("unit".to_string()),
            one_of: todo!(),
            enumeration: Some(vec![json!("hello"), json!(3)]),
            read_only: true,
            write_only: true,
            format: Some("format".to_string()),
            subtype: Some(DataSchemaSubtype::Object(ObjectSchema {
                properties: Some(
                    [(
                        "hello".to_string(),
                        DataSchema {
                            subtype: Some(DataSchemaSubtype::Array(ArraySchema {
                                items: Some(BoxedElemOrVec::Elem(Box::new(DataSchema {
                                    subtype: Some(DataSchemaSubtype::Integer(
                                        IntegerSchema::default(),
                                    )),
                                    ..Default::default()
                                }))),
                                ..Default::default()
                            })),
                            ..Default::default()
                        },
                    )]
                    .into_iter()
                    .collect(),
                ),
                required: Some(vec!["hello".to_string()]),
                other: (),
            })),
            other: (),
        })
        .unwrap();
    }
}
