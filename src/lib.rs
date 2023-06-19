//! Web of Things consumer
//!
//! Provides all the building blocks to consume (use) [Web Of Things](https://www.w3.org/WoT/) Things.

mod data_schema_validator;
mod sealed;

use core::slice;
use std::{
    array,
    convert::identity,
    fmt::{self, Display},
    future::Future,
    marker::PhantomData,
    num::NonZeroU64,
    ops::{BitOr, Not},
    rc::Rc,
    str::FromStr,
};

use bitflags::bitflags;
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

    properties.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    let base = td.base.clone();
    Ok(Consumer {
        base,
        properties,
        client,
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
}

#[derive(Debug)]
pub struct Consumer {
    base: Option<String>,
    properties: Vec<Property>,
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
        let inner = PropertyReaderInner {
            property: self.property,
            forms: PartialVecRefs::Whole(&self.property.forms),
            uri_variables: Vec::new(),
            security: None,
            base: self.base,
            client: self.client,
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
    status: Result<PropertyReaderInner<'a>, PropertyReaderError<'a>>,
    _marker: PhantomData<fn() -> T>,
}

impl<'a, T> PropertyReader<'a, T> {
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

        if let Err(err) = validate_uri_variable_value(&uri_variable.schema, &value) {
            self.status = Err(PropertyReaderError {
                property: inner.property,
                kind: PropertyReaderErrorKind::InvalidUriVariable {
                    uri_variable: uri_variable.name.clone(),
                    kind: err,
                },
            });
            return self;
        };

        inner.uri_variables.push((name.into(), value));
        self
    }

    pub fn send(
        &self,
    ) -> Result<
        impl Future<Output = Result<T, PropertyReaderSendFutureError>> + '_,
        PropertyReaderSendError<'_>,
    >
    where
        T: DeserializeOwned,
    {
        let PropertyReaderInner {
            property,
            base,
            forms,
            uri_variables,
            security,
            client,
        } = self
            .status
            .as_ref()
            .map_err(|err| PropertyReaderSendError::PropertyReader(err.clone()))?;

        let mut forms = forms
            .iter()
            .filter(|form| form.op.contains(&FormOperation::ReadProperty));
        let form = forms.next().ok_or(PropertyReaderSendError::NoForms)?;
        if forms.next().is_some() {
            return Err(PropertyReaderSendError::MultipleChoices);
        }

        create_property_reader_send_future(
            form,
            base,
            uri_variables,
            security,
            &property.data_schema,
            client,
        )
        .map_err(PropertyReaderSendError::InitFuture)
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
    client: &'a reqwest::Client,
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
pub struct PropertyReaderError<'a> {
    property: &'a Property,
    pub kind: PropertyReaderErrorKind,
}

#[derive(Debug, Clone)]
pub enum PropertyReaderErrorKind {
    UnavailableSecurityScheme(ReaderSecurityScheme),
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

fn create_property_reader_send_future<'a, T>(
    form: &Form,
    base: &Option<String>,
    uri_variables: &[(String, UriVariableValue)],
    security: &'a Option<StatefulSecurityScheme>,
    data_schema: &'a DataSchema<(), (), ()>,
    client: &reqwest::Client,
) -> Result<
    impl Future<Output = Result<T, PropertyReaderSendFutureError>> + 'a,
    PropertyReaderSendFutureInitError,
>
where
    T: DeserializeOwned + 'a,
{
    let url = form
        .href
        .parse::<Url>()
        .or_else(|err| match err {
            url::ParseError::RelativeUrlWithoutBase => base
                .as_ref()
                .ok_or_else(|| {
                    PropertyReaderSendFutureInitError::RelativeUrlWithoutBase(form.href.clone())
                })
                .and_then(|base| {
                    base.parse::<Url>().map_err(|source| {
                        PropertyReaderSendFutureInitError::InvalidBase {
                            base: base.to_string(),
                            href: form.href.clone(),
                            source,
                        }
                    })
                })
                .and_then(|base| {
                    base.join(&form.href).map_err(|source| {
                        PropertyReaderSendFutureInitError::InvalidJoinedHref {
                            base,
                            href: form.href.clone(),
                            source,
                        }
                    })
                }),
            _ => Err(PropertyReaderSendFutureInitError::InvalidHref {
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
        Some(content_type) => Err(PropertyReaderSendFutureInitError::InvalidResponse(
            PropertyReaderSendFutureInvalidResponse::UnsupportedFormat(content_type.clone()),
        )),
    }
}

async fn perform_request<'a, T, F, Fut>(
    builder: reqwest::RequestBuilder,
    response_handler: F,
    data_schema: &'a DataSchema<(), (), ()>,
) -> Result<T, PropertyReaderSendFutureError>
where
    F: 'a + FnOnce(reqwest::Response, &'a DataSchema<(), (), ()>) -> Fut,
    Fut: 'a + Future<Output = Result<T, PropertyReaderSendFutureInvalidResponse>>,
{
    let response = builder
        .send()
        .await
        .map_err(PropertyReaderSendFutureError::RequestError)?
        .error_for_status()
        .map_err(PropertyReaderSendFutureError::ResponseError)?;

    response_handler(response, data_schema)
        .await
        .map_err(PropertyReaderSendFutureError::InvalidResponse)
}

#[derive(Debug)]
pub enum PropertyReaderSendError<'a> {
    PropertyReader(PropertyReaderError<'a>),
    MultipleChoices,
    NoForms,
    InitFuture(PropertyReaderSendFutureInitError),
}

#[derive(Debug)]
pub enum PropertyReaderSendFutureInitError {
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
    InvalidResponse(PropertyReaderSendFutureInvalidResponse),
}

#[derive(Debug)]
pub enum PropertyReaderSendFutureError {
    InvalidResponse(PropertyReaderSendFutureInvalidResponse),
    RequestError(reqwest::Error),
    ResponseError(reqwest::Error),
}

#[derive(Debug)]
pub enum PropertyReaderSendFutureInvalidResponse {
    Json(HandleJsonResponseError),
    UnsupportedFormat(String),
}

#[inline]
async fn handle_json_response<T>(
    response: reqwest::Response,
    data_schema: &DataSchema<(), (), ()>,
) -> Result<T, PropertyReaderSendFutureInvalidResponse>
where
    T: DeserializeOwned,
{
    handle_json_response_inner(response, data_schema)
        .await
        .map_err(PropertyReaderSendFutureInvalidResponse::Json)
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

    handle_json_response_validate(&data, data_schema)
        .map_err(HandleJsonResponseRefError::into_owned)?;
    serde_json::from_value(data).map_err(HandleJsonResponseError::Deserialization)
}

struct ResponseValidatorState<'a> {
    responses: &'a [serde_json::Value],
    branching: ResponseValidatorBranching<'a>,
    logic: ResponseValidatorLogic,
}

fn handle_json_response_validate<'a>(
    response: &'a serde_json::Value,
    data_schema: &'a DataSchema<(), (), ()>,
) -> Result<(), HandleJsonResponseRefError<'a>> {
    let responses = array::from_ref(response).as_slice();
    let mut queue: SmallVec<[_; 8]> = smallvec![ResponseValidatorState {
        responses,
        branching: ResponseValidatorBranching::Unchecked(data_schema),
        logic: ResponseValidatorLogic::And { parent: None },
    }];

    while let Some(ResponseValidatorState {
        responses,
        branching,
        logic,
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

                queue.push(ResponseValidatorState {
                    responses,
                    branching: ResponseValidatorBranching::Evaluated(data_schema),
                    logic,
                });

                // From last to first, in order to pop from the first to last
                responses
                    .iter()
                    .rev()
                    .try_for_each(|response| {
                        handle_json_response_validate_impl(response, data_schema, &mut queue)
                    })
                    .or_else(|err| handle_response_validate_error(err, logic, &mut queue))?;
            }

            ResponseValidatorBranching::Evaluated(..) => {}

            ResponseValidatorBranching::UnevaluatedOneOf(one_of) => {
                let parent = queue.len();
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
                            responses,
                            branching: ResponseValidatorBranching::Unchecked(data_schema),
                            logic: ResponseValidatorLogic::Or { parent },
                        }),
                );
            }

            ResponseValidatorBranching::EvaluatedOneOf(one_of) => {
                handle_response_validate_error(
                    HandleJsonResponseRefError::OneOf {
                        expected: one_of,
                        found: response,
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
    Evaluated(&'a DataSchema<(), (), ()>),
    UnevaluatedOneOf(&'a [DataSchema<(), (), ()>]),
    EvaluatedOneOf(&'a [DataSchema<(), (), ()>]),
}

fn handle_json_response_validate_impl<'a, const N: usize>(
    response: &'a serde_json::Value,
    data_schema: &'a DataSchema<(), (), ()>,
    queue: &mut SmallVec<[ResponseValidatorState<'a>; N]>,
) -> Result<(), HandleJsonResponseRefError<'a>> {
    if let Some(constant) = &data_schema.constant {
        if response != constant {
            return Err(HandleJsonResponseRefError::Constant {
                expected: constant,
                found: response,
            });
        }
    }

    if let Some(enumeration) = &data_schema.enumeration {
        if enumeration.iter().any(|variant| response == variant).not() {
            return Err(HandleJsonResponseRefError::Enumeration {
                expected: enumeration,
                found: response,
            });
        }
    }

    let parent = queue.len() - 1;
    debug_assert!(matches!(
        queue[parent].branching,
        ResponseValidatorBranching::Evaluated(queue_data_schema)
        if std::ptr::eq(queue_data_schema, data_schema)
    ));
    let parent = Some(parent);

    if let Some(subtype) = &data_schema.subtype {
        use serde_json::Value;

        match (subtype, response) {
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
                            responses: responses.as_slice(),
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
                                    responses: array::from_ref(response).as_slice(),
                                    branching: ResponseValidatorBranching::Unchecked(data_schema),
                                    logic: ResponseValidatorLogic::And { parent },
                                },
                            ));
                        }
                    }
                }
            }

            (DataSchemaSubtype::Number(data_schema), Value::Number(response)) => {
                let Some(value) = response.as_f64() else {
                    return Err(HandleJsonResponseRefError::NumberSubtype(
                        NumericSubtypeError::InvalidType(response.clone()),
                    ));
                };

                check_number(data_schema, value)
                    .map_err(|err| HandleJsonResponseRefError::NumberSubtype(err.into()))?;
            }
            (DataSchemaSubtype::Integer(data_schema), Value::Number(response)) => {
                let Some(value) = response.as_i64() else {
                    return Err(HandleJsonResponseRefError::IntegerSubtype(
                        NumericSubtypeError::InvalidType(response.clone()),
                    ));
                };

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
                                responses: array::from_ref(response).as_slice(),
                                branching: ResponseValidatorBranching::Unchecked(property),
                                logic: ResponseValidatorLogic::And { parent },
                            }),
                    );
                }
            }

            _ => {
                return Err(HandleJsonResponseRefError::Subtype {
                    expected: subtype.into(),
                    found: response,
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
    TooManyChildren,
    TooDeep,
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
    TooManyChildren,
    TooDeep,
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
            Self::TooManyChildren => HandleJsonResponseError::TooManyChildren,
            Self::TooDeep => HandleJsonResponseError::TooDeep,
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
                let &UriVariableValue::Integer(value) = value else {
                    return Err(InvalidUriVariableKind::Subtype(
                        InvalidUriVariableKindSubtype::Integer(
                            InvalidUriVariableKindSubtypeNumeric::Type
                        )
                    ));
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

#[cfg(test)]
mod tests {
    use reqwest::Client;
    use wot_td::{
        builder::{BuildableInteractionAffordance, SpecializableDataSchema},
        Thing,
    };

    use super::*;

    #[tokio::test]
    async fn get_simple_property() {
        let client = Client::new();
        let td = Thing::builder("test")
            .finish_extend()
            .property("prop1", |b| {
                b.finish_extend_data_schema()
                    .integer()
                    .form(|b| b.href("/test"))
            })
            .build()
            .unwrap();

        let prop = consume(&td, client)
            .await
            .unwrap()
            .property("test")
            .unwrap()
            .read()
            .send()
            .unwrap()
            .await
            .unwrap();

        todo!()
    }
}
