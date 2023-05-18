//! Web of Things consumer
//!
//! Provides all the building blocks to consume (use) [Web Of Things](https://www.w3.org/WoT/) Things.

mod data_schema_validator;

use std::{
    convert::identity,
    marker::PhantomData,
    ops::{BitOr, Not},
    rc::Rc,
    str::FromStr,
};

use bitflags::bitflags;
use wot_td::{
    extend::ExtendableThing,
    thing::{
        self, ArraySchema, DataSchema, DataSchemaSubtype, DefaultedFormOperations, FormOperation,
        IntegerSchema, InteractionAffordance, NumberSchema, ObjectSchema, PropertyAffordance,
        StringSchema,
    },
    Thing,
};
use zeroize::{Zeroize, ZeroizeOnDrop};

pub async fn consume<Other>(td: &Thing<Other>) -> Result<Consumer, ConsumeError>
where
    Other: ExtendableThing,
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
    Ok(Consumer { properties })
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
    properties: Vec<Property>,
    // TODO: other
}

impl Consumer {
    #[inline]
    pub fn property(&self, name: &str) -> Option<&Property> {
        self.properties
            .binary_search_by(|prop| prop.name.as_str().cmp(name))
            .ok()
            .map(|index| &self.properties[index])
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
}

impl Form {
    fn try_from_td_form<Other>(
        form: &thing::Form<Other>,
        op: Vec<FormOperation>,
        schema_definitions: &[SchemaDefinition],
    ) -> Result<Self, InvalidForm>
    where
        Other: ExtendableThing,
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
            other: _,
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

        Ok(Self {
            href,
            op,
            content_type,
            content_coding,
            subprotocol,
            security_scheme,
            expected_response_content_type,
            additional_expected_response,
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
    status: Result<PropertyReaderInner<'a>, PropertyReaderError>,
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
                self.status = Err(PropertyReaderError::UnavailableProtocols);
            }
        }
        self
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
                    self.status = Err(PropertyReaderError::UnavailableSecurityScheme(
                        security_scheme.to_stateless(),
                    ));
                } else {
                    inner.security = Some(security_scheme);
                }
            } else {
                self.status = Err(PropertyReaderError::SecurityAlreadySet);
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
    uri_variables: Vec<(String, ScalarDataSchema)>,
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
pub enum PropertyReaderError {
    UnavailableSecurityScheme(ReaderSecurityScheme),
    UnavailableProtocols,
    SecurityAlreadySet,
}

#[derive(Debug)]
struct SetUriVariable<'a> {
    name: String,
    schema: &'a ScalarDataSchema,
    value: UriVariableValue,
}

#[derive(Debug)]
enum UriVariableValue {
    String(String),
    Integer(i64),
    Number(f64),
    Boolean(bool),
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

#[derive(Debug)]
pub struct PropertyReaderSendFuture;

impl PropertyReaderSendFuture {
    fn new(
        form: &Form,
        base: &Option<String>,
        uri_variables: &[(String, ScalarDataSchema)],
        security: &StatefulSecurityScheme,
    ) -> Self {
        // TODO: move instantiation of reqwest client upstream
        let client = reqwest::Client::new();

        todo!()
    }
}

#[derive(Debug)]
pub enum PropertyReaderSendError {
    PropertyReader(PropertyReaderError),
    MultipleChoices,
}
