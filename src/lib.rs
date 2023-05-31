//! Web of Things consumer
//!
//! Provides all the building blocks to consume (use) [Web Of Things](https://www.w3.org/WoT/) Things.

mod data_schema_validator;

use std::{
    cmp,
    convert::identity,
    fmt::{self, Display},
    future::Future,
    marker::PhantomData,
    ops::{BitOr, Not},
    rc::Rc,
    str::FromStr,
};

use bitflags::bitflags;
use reqwest::Url;
use sealed::HasHttpProtocolExtension;
use wot_td::{
    extend::ExtendableThing,
    protocol::http,
    thing::{
        self, ArraySchema, DataSchema, DataSchemaSubtype, DefaultedFormOperations, FormOperation,
        IntegerSchema, InteractionAffordance, Minimum, NumberSchema, ObjectSchema,
        PropertyAffordance, StringSchema,
    },
    Thing,
};
use zeroize::{Zeroize, ZeroizeOnDrop};

pub async fn consume<Other>(td: &Thing<Other>) -> Result<Consumer, ConsumeError>
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
                                    let schema = ScalarDataSchema::try_from_data_schema(
                                        schema,
                                        &schema_definitions,
                                    )
                                    .map_err(|err| ConsumeError::UriVariable {
                                        property: property_name.clone(),
                                        name: name.clone(),
                                        source: err,
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

    properties.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    let base = td.base.clone();
    Ok(Consumer { base, properties })
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
}

#[derive(Debug)]
pub struct Consumer {
    base: Option<String>,
    properties: Vec<Property>,
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
                PropertyRef { property, base }
            })
    }
}

#[derive(Debug)]
pub struct PropertyRef<'a> {
    property: &'a Property,
    base: &'a Option<String>,
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
        let inner = PropertyReaderInner {
            property: self.property,
            forms: PartialVecRefs::Whole(&self.property.forms),
            uri_variables: Vec::new(),
            security: None,
            base: self.base,
        };
        PropertyReader {
            status: Ok(inner),
            _marker: PhantomData,
        }
    }
}

#[derive(Debug)]
struct UriVariable {
    name: String,
    schema: ScalarDataSchema,
}

#[derive(Debug)]
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
        op: Vec<FormOperation>,
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

#[derive(Debug)]
pub struct AdditionalExpectedResponse {
    success: bool,
    content_type: Option<String>,
    // TODO: should we use an `Arc`?
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

impl ScalarDataSchema {
    fn try_from_data_schema<DS, AS, OS>(
        value: &DataSchema<DS, AS, OS>,
        schema_definitions: &[SchemaDefinition],
    ) -> Result<Self, NonScalarDataSchema> {
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
            .map(|one_of| {
                one_of
                    .iter()
                    .map(|schema| Self::try_from_data_schema(schema, schema_definitions))
                    .collect()
            })
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
                .map(|items| items.iter().map(data_schema_without_extensions).collect());
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
    status: Result<PropertyReaderInner<'a>, PropertyReaderError<'a>>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> PropertyReader<'_, T> {
    pub fn no_security(&mut self) -> &mut Self {
        self.filter_security_scheme(StatefulSecurityScheme::NoSecurity)
    }

    pub fn basic(&mut self, username: impl Into<String>, password: impl Into<String>) -> &mut Self {
        let username = username.into();
        let password = password.into();

        self.filter_security_scheme(StatefulSecurityScheme::Basic { username, password })
    }

    pub fn bearer(&mut self, token: impl Into<String>) -> &mut Self {
        let token = token.into();

        self.filter_security_scheme(StatefulSecurityScheme::Bearer(token))
    }

    #[inline]
    pub fn protocol(&mut self, accepted_protocol: Protocol) -> &mut Self {
        self.protocols([accepted_protocol].as_slice())
    }

    pub fn protocols(&mut self, accepted_protocols: &[Protocol]) -> &mut Self {
        if let Ok(inner) = &mut self.status {
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
                self.status = Err(PropertyReaderError {
                    property: inner.property,
                    kind: PropertyReaderErrorKind::UnavailableProtocols,
                });
            }
        }
        self
    }

    // TODO: check if we can make `value: impl TryInto<UriVariableValue>`, the type of error could
    // be annyoing because of the `Infallible` conversions.
    pub fn uri_variable<N>(&mut self, name: N, value: UriVariableValue) -> &mut Self
    where
        N: Into<String> + AsRef<str>,
    {
        let Ok(inner) = &mut self.status else {
            return self;
        };

        let name_str = name.as_ref();
        let uri_variable = inner
            .property
            .uri_variables
            .binary_search_by(|uri_variable| uri_variable.name.as_str().cmp(name_str))
            .map(|index| &inner.property.uri_variables[index]);

        let Ok(uri_variable) = uri_variable else {
            self.status = Err(PropertyReaderError {
                property: inner.property,
                kind: PropertyReaderErrorKind::UnavailableUriVariable(name.into()),
            });
            return self;
        };

        validate_uri_variable_value(uri_variable, value);
        todo!()
    }

    pub fn send(&self) -> Result<PropertyReaderSendFuture, PropertyReaderSendError> {
        todo!()
    }

    fn filter_security_scheme(&mut self, security_scheme: StatefulSecurityScheme) -> &mut Self {
        if let Ok(inner) = &mut self.status {
            if inner.security.is_none() {
                inner
                    .forms
                    .retain(|form| form.security_scheme.contains(security_scheme.to_flag()));

                if inner.forms.is_empty() {
                    self.status = Err(PropertyReaderError {
                        property: inner.property,
                        kind: PropertyReaderErrorKind::UnavailableSecurityScheme(
                            security_scheme.to_stateless(),
                        ),
                    });
                } else {
                    inner.security = Some(security_scheme);
                }
            } else {
                self.status = Err(PropertyReaderError {
                    property: inner.property,
                    kind: PropertyReaderErrorKind::SecurityAlreadySet,
                });
            }
        }

        self
    }
}

#[derive(Debug)]
struct PropertyReaderInner<'a> {
    property: &'a Property,
    base: &'a Option<String>,
    forms: PartialVecRefs<'a, Form>,
    uri_variables: Vec<(String, UriVariableValue)>,
    security: Option<StatefulSecurityScheme>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReaderSecurityScheme {
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
    fn to_stateless(&self) -> ReaderSecurityScheme {
        match self {
            Self::NoSecurity => ReaderSecurityScheme::Basic,
            Self::Basic { .. } => ReaderSecurityScheme::Basic,
            Self::Bearer(_) => ReaderSecurityScheme::Bearer,
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

impl<T> PartialVecRefs<'_, T> {
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

    fn len(&self) -> usize {
        match self {
            Self::Whole(data) => data.len(),
            Self::Some(data) => data.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Whole(data) => data.is_empty(),
            Self::Some(data) => data.is_empty(),
        }
    }
}

#[derive(Debug)]
pub struct PropertyReaderError<'a> {
    property: &'a Property,
    pub kind: PropertyReaderErrorKind,
}

#[derive(Debug)]
pub enum PropertyReaderErrorKind {
    UnavailableSecurityScheme(ReaderSecurityScheme),
    UnavailableProtocols,
    SecurityAlreadySet,
    UnavailableUriVariable(String),
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

pub struct PropertyReaderSendFuture(
    Result<
        Box<dyn Future<Output = Result<reqwest::Response, reqwest::Error>>>,
        PropertyReaderSendFutureError,
    >,
);

impl PropertyReaderSendFuture {
    fn new(
        form: &Form,
        base: &Option<String>,
        uri_variables: &[(String, UriVariableValue)],
        security: &StatefulSecurityScheme,
    ) -> Self {
        // TODO: move instantiation of reqwest client upstream
        let client = reqwest::Client::new();

        let url = form
            .href
            .parse::<Url>()
            .or_else(|err| match err {
                url::ParseError::RelativeUrlWithoutBase => base
                    .as_ref()
                    .ok_or_else(|| {
                        PropertyReaderSendFutureError::RelativeUrlWithoutBase(form.href.clone())
                    })
                    .and_then(|base| {
                        base.parse::<Url>().map_err(|source| {
                            PropertyReaderSendFutureError::InvalidBase {
                                base: base.to_string(),
                                href: form.href.clone(),
                                source,
                            }
                        })
                    })
                    .and_then(|base| {
                        base.join(&form.href).map_err(|source| {
                            PropertyReaderSendFutureError::InvalidJoinedHref {
                                base,
                                href: form.href.clone(),
                                source,
                            }
                        })
                    }),
                _ => Err(PropertyReaderSendFutureError::InvalidHref {
                    href: form.href.clone(),
                    source: err,
                }),
            })
            .map(|mut url| {
                use std::fmt::Write;

                let mut query_pairs = url.query_pairs_mut();
                let mut buffer = String::new();

                for (name, value) in uri_variables {
                    buffer.clear();
                    write!(buffer, "{}", value).unwrap();
                    query_pairs.append_pair(name, &buffer);
                }

                drop(query_pairs);
                url
            });
        let url = match url {
            Ok(url) => url,
            Err(err) => return Self(Err(err)),
        };

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

        todo!()
    }
}

#[derive(Debug)]
pub enum PropertyReaderSendError<'a> {
    PropertyReader(PropertyReaderError<'a>),
    MultipleChoices,
}

#[derive(Debug)]
pub enum PropertyReaderSendFutureError {
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
}

fn validate_uri_variable_value(
    schema: &ScalarDataSchema,
    value: &UriVariableValue,
) -> Result<(), InvalidUriVariableKind> {
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
                let UriVariableValue::Number(value) = value else {
                    return Err(InvalidUriVariableKind::Subtype(
                        InvalidUriVariableKindSubtype::Number(
                            InvalidUriVariableKindSubtypeNumber::Type
                        )
                    ));
                };

                if let Some(minimum) = &schema.minimum {
                    if is_above_minimum(value, minimum).not() {
                        return Err(InvalidUriVariableKind::Subtype(
                            InvalidUriVariableKindSubtype::Number(
                                InvalidUriVariableKindSubtypeNumber::BelowMinimum(minimum.clone()),
                            ),
                        ));
                    }

                    todo!()
                }

                todo!()
            }
            ScalarDataSchemaSubtype::Integer(_) => todo!(),
            ScalarDataSchemaSubtype::String(_) => todo!(),
            ScalarDataSchemaSubtype::Null => todo!(),
        }
    }

    todo!()
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

#[derive(Debug)]
pub enum InvalidUriVariableKind {
    Constant(serde_json::Value),
    Enumeration(Vec<serde_json::Value>),
    Subtype(InvalidUriVariableKindSubtype),
}

#[derive(Debug)]
pub enum InvalidUriVariableKindSubtype {
    Boolean,
    Number(InvalidUriVariableKindSubtypeNumber),
    Integer(InvalidUriVariableKindSubtypeInteger),
    String(InvalidUriVariableKindSubtypeString),
    Null,
}

#[derive(Debug)]
pub enum InvalidUriVariableKindSubtypeNumber {
    Type,
    BelowMinimum(Minimum<f64>),
}

#[derive(Debug)]
pub enum InvalidUriVariableKindSubtypeInteger {
    Type,
}

#[derive(Debug)]
pub enum InvalidUriVariableKindSubtypeString {
    Type,
}

mod sealed {
    use wot_td::{
        extend::ExtendableThing,
        hlist::{self, HListRef, NonEmptyHList},
        protocol::http::{self, HttpProtocol},
    };

    pub trait HasHttpProtocolExtension: Sized + ExtendableThing {
        fn http_protocol(&self) -> &HttpProtocol;
        fn http_protocol_form(form: &Self::Form) -> &http::Form;
    }

    impl HasHttpProtocolExtension for HttpProtocol {
        #[inline]
        fn http_protocol(&self) -> &HttpProtocol {
            self
        }

        #[inline]
        fn http_protocol_form(form: &Self::Form) -> &http::Form {
            form
        }
    }

    macro_rules! impl_hlist_with_http_protocol_in_head {
        (
            @make_hlist
        ) => {
            hlist::Cons<HttpProtocol>
        };

        (
            @make_hlist
            $generic:ident $(, $($rest:tt)*)?
        ) => {
           hlist::Cons<
               $generic,
               impl_hlist_with_http_protocol_in_head!(@make_hlist $($($rest)*)?),
            >
        };

        (
            @make_impl
            $(
                $($generic:ident),+
            )?
        ) => {
            impl $(< $($generic),+ >)? HasHttpProtocolExtension for impl_hlist_with_http_protocol_in_head!(@make_hlist $($($generic),+)?)
            $(
                where
                    $(
                        $generic : wot_td::extend::ExtendableThing
                    ),+
            )?
            {
                #[inline]
                fn http_protocol(&self) -> &HttpProtocol {
                    self.to_ref().split_last().0
                }

                #[inline]
                fn http_protocol_form(form: &Self::Form) -> &http::Form {
                    form.to_ref().split_last().0
                }
            }
        };

        (
            $(
                $($generic:ident),* $(,)?
            );+
        ) => {
            $(
                impl_hlist_with_http_protocol_in_head!(@make_impl $($generic),*);
            )*
        };
    }

    impl_hlist_with_http_protocol_in_head!(
        A;
        A, B;
        A, B, C;
        A, B, C, D;
        A, B, C, D, E;
        A, B, C, D, E, F;
        A, B, C, D, E, F, G;
        A, B, C, D, E, F, G, H;
        A, B, C, D, E, F, G, H, I;
        A, B, C, D, E, F, G, H, I, J;
        A, B, C, D, E, F, G, H, I, J, K;
        A, B, C, D, E, F, G, H, I, J, K, L;
        A, B, C, D, E, F, G, H, I, J, K, L, M;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, AB;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, AB, AC;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, AB, AC, AD;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, AB, AC, AD, AE;
        A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, AA, AB, AC, AD, AE, AF;
    );

    #[cfg(test)]
    mod tests {
        use wot_td::protocol::coap::CoapProtocol;

        use super::*;

        #[test]
        fn http_protocol() {
            let proto = HttpProtocol {};
            assert!(std::ptr::eq(proto.http_protocol(), &proto));

            let list = hlist::Nil::cons(HttpProtocol {})
                .cons(CoapProtocol {})
                .cons(CoapProtocol {})
                .cons(CoapProtocol {});
            let _http: &HttpProtocol = list.http_protocol();
        }
    }
}
