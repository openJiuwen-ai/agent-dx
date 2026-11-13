use super::types::{bounded, email_address, identifier};
use super::{Error, OutlookRequest};
use reqwest::Url;
use serde_json::{Map, Value};
use std::collections::BTreeMap;

const LIST_FIELDS: &str = "id,conversationId,webLink";
const HEADER_FIELDS: &str =
    "id,conversationId,webLink,subject,sender,toRecipients,receivedDateTime";
const BODY_FIELDS: &str =
    "id,conversationId,webLink,subject,sender,toRecipients,receivedDateTime,body,bodyPreview";

pub(super) fn message_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 2048
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_+=-".contains(&c))
}

// Canonical UTC seconds only, matching the existing Python consumer. Reject
// invalid calendar dates instead of letting them change the Graph query.
fn utc_seconds(value: &str) -> bool {
    if value.len() != 20 || !value.is_ascii() {
        return false;
    }
    let bytes = value.as_bytes();
    if [
        bytes[4], bytes[7], bytes[10], bytes[13], bytes[16], bytes[19],
    ] != *b"--T::Z"
    {
        return false;
    }
    let fields = [
        &value[..4],
        &value[5..7],
        &value[8..10],
        &value[11..13],
        &value[14..16],
        &value[17..19],
    ];
    let mut parsed = [0u16; 6];
    for (index, field) in fields.iter().enumerate() {
        if !field.bytes().all(|c| c.is_ascii_digit()) {
            return false;
        }
        let Ok(number) = field.parse() else {
            return false;
        };
        parsed[index] = number;
    }
    let [year, month, day, hour, minute, second] = parsed;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return false,
    };
    year > 0 && (1..=days).contains(&day) && hour < 24 && minute < 60 && second < 60
}

impl OutlookRequest {
    pub(super) fn validate(&self) -> Result<(), Error> {
        let valid = match self {
            Self::Search {
                sender,
                since,
                until,
                limit,
                page,
            } => {
                (sender.is_empty() || email_address(sender))
                    && (since.is_empty() || utc_seconds(since))
                    && (until.is_empty() || utc_seconds(until))
                    && (since.is_empty() || until.is_empty() || since < until)
                    && (1..=100).contains(limit)
                    && bounded(page, 8192)
            }
            Self::Read { message_id: id, .. } => message_id(id),
        };
        if valid {
            Ok(())
        } else {
            Err(Error::InvalidRequest)
        }
    }

    pub(super) fn endpoint(&self, subject: &str) -> Result<Url, Error> {
        if !identifier(subject, 256) {
            return Err(Error::AccountMismatch);
        }
        self.validate()?;
        let mut url = Url::parse("https://graph.microsoft.com/v1.0/users")
            .map_err(|_| Error::NotConfigured)?;
        url.path_segments_mut()
            .map_err(|_| Error::NotConfigured)?
            .push(subject)
            .push("messages");
        match self {
            Self::Search {
                sender,
                since,
                until,
                limit,
                page,
            } => {
                url.query_pairs_mut()
                    .append_pair("$top", &limit.to_string())
                    .append_pair("$select", LIST_FIELDS);
                let mut filters = Vec::new();
                if !sender.is_empty() {
                    filters.push(format!("from/emailAddress/address eq '{sender}'"));
                }
                if !since.is_empty() {
                    filters.push(format!("receivedDateTime ge {since}"));
                }
                if !until.is_empty() {
                    filters.push(format!("receivedDateTime lt {until}"));
                }
                if !filters.is_empty() {
                    url.query_pairs_mut()
                        .append_pair("$filter", &filters.join(" and "));
                }
                if !page.is_empty() {
                    return checked_page(page, &url);
                }
            }
            Self::Read { message_id, body } => {
                url.path_segments_mut()
                    .map_err(|_| Error::NotConfigured)?
                    .push(message_id);
                url.query_pairs_mut()
                    .append_pair("$select", if *body { BODY_FIELDS } else { HEADER_FIELDS });
            }
        }
        Ok(url)
    }

    pub(super) fn project(&self, value: Value, subject: &str) -> Result<Value, Error> {
        match self {
            Self::Read { message_id, body } => {
                if value.get("id").and_then(Value::as_str) != Some(message_id.as_str()) {
                    return Err(Error::InvalidResponse);
                }
                project_fields(value, if *body { BODY_FIELDS } else { HEADER_FIELDS })
            }
            Self::Search {
                sender,
                since,
                until,
                limit,
                ..
            } => {
                let values = value
                    .get("value")
                    .and_then(Value::as_array)
                    .ok_or(Error::InvalidResponse)?;
                if values.len() > usize::from(*limit) {
                    return Err(Error::InvalidResponse);
                }
                let messages = values
                    .iter()
                    .cloned()
                    .map(|item| project_fields(item, LIST_FIELDS))
                    .collect::<Result<Vec<_>, _>>()?;
                let mut result = Map::new();
                result.insert("value".into(), Value::Array(messages));
                if let Some(page) = value.get("@odata.nextLink") {
                    let page = page.as_str().ok_or(Error::InvalidResponse)?;
                    let initial = Self::Search {
                        sender: sender.clone(),
                        since: since.clone(),
                        until: until.clone(),
                        limit: *limit,
                        page: String::new(),
                    }
                    .endpoint(subject)?;
                    checked_page(page, &initial).map_err(|_| Error::InvalidResponse)?;
                    result.insert("@odata.nextLink".into(), Value::String(page.into()));
                }
                Ok(Value::Object(result))
            }
        }
    }
}

fn project_fields(value: Value, fields: &str) -> Result<Value, Error> {
    let object = value.as_object().ok_or(Error::InvalidResponse)?;
    if !object
        .get("id")
        .and_then(Value::as_str)
        .is_some_and(message_id)
    {
        return Err(Error::InvalidResponse);
    }
    Ok(Value::Object(
        fields
            .split(',')
            .filter_map(|field| {
                object
                    .get(field)
                    .map(|value| (field.to_string(), value.clone()))
            })
            .collect(),
    ))
}

fn query(url: &Url) -> Result<BTreeMap<String, String>, Error> {
    let mut result = BTreeMap::new();
    for (key, value) in url.query_pairs() {
        if result
            .insert(key.into_owned(), value.into_owned())
            .is_some()
        {
            return Err(Error::InvalidRequest);
        }
    }
    Ok(result)
}

// The page URL remains the provider's opaque nextLink. Validate its entire
// mailbox/query projection; never follow arbitrary Graph resources or widen
// select/filter/top. Do not extract skip and reconstruct a different request.
fn checked_page(page: &str, initial: &Url) -> Result<Url, Error> {
    if !bounded(page, 8192) {
        return Err(Error::InvalidRequest);
    }
    let url = Url::parse(page).map_err(|_| Error::InvalidRequest)?;
    if url.scheme() != "https"
        || url.host_str() != Some("graph.microsoft.com")
        || url.port().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.path() != initial.path()
    {
        return Err(Error::InvalidRequest);
    }
    let mut actual = query(&url)?;
    let skip = actual.remove("$skip");
    let token = actual.remove("$skiptoken");
    let valid_cursor = match (&skip, &token) {
        (Some(skip), None) => {
            !skip.is_empty() && skip.len() <= 20 && skip.bytes().all(|c| c.is_ascii_digit())
        }
        (None, Some(token)) => !token.is_empty() && bounded(token, 4096),
        _ => false,
    };
    if !valid_cursor || actual != query(initial)? {
        return Err(Error::InvalidRequest);
    }
    Ok(url)
}
