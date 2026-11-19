//! Parser for the Warp 10 `/api/v0/update` line format:
//!   <ts>[/<lat:lon|rawloc>[/<elev>]] <class>{<k>=<v>,...} <value>
//!
//! Timestamps are microseconds since epoch; a leading '+'/'-' makes them
//! relative to `now_us` (now + ts, in µs); `now` means now_us.

use crate::geoxp::{to_geo_xp_point, NO_ELEVATION, NO_LOCATION};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Long(i64),
    Double(f64),
    Str(String),
    Bool(bool),
    /// null value in the line format = delete marker
    Delete,
}

#[derive(Debug, Clone)]
pub struct Datum {
    pub ts: i64,
    pub location: i64,
    pub elevation: i64,
    pub class: String,
    pub labels: BTreeMap<String, String>,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for ParseError {}

fn unquote(s: &str) -> Result<String, ParseError> {
    let b = s.as_bytes();
    if b.len() >= 2 && (b[0] == b'\'' || b[0] == b'"') && b[b.len() - 1] == b[0] {
        let inner = &s[1..s.len() - 1];
        // '' or "" inside a quoted string is an escaped quote
        Ok(inner.replace("''", "'").replace("\"\"", "\""))
    } else {
        Ok(s.to_string())
    }
}

fn parse_labels(s: &str) -> Result<BTreeMap<String, String>, ParseError> {
    let mut labels = BTreeMap::new();
    let mut cur = String::new();
    let mut in_quote: Option<u8> = None;
    let mut parts = Vec::new();
    for c in s.chars() {
        match in_quote {
            Some(q) => {
                cur.push(c);
                if c as u8 == q {
                    in_quote = None;
                }
            }
            None => {
                if c == '\'' || c == '"' {
                    in_quote = Some(c as u8);
                    cur.push(c);
                } else if c == ',' {
                    parts.push(std::mem::take(&mut cur));
                } else {
                    cur.push(c);
                }
            }
        }
    }
    if !cur.is_empty() {
        parts.push(cur);
    }
    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let eq = part.find('=').ok_or_else(|| ParseError(format!("bad label '{part}'")))?;
        let k = unquote(part[..eq].trim())?;
        let v = unquote(part[eq + 1..].trim())?;
        labels.insert(k, v);
    }
    Ok(labels)
}

fn parse_ts(s: &str, now_us: i64) -> Result<i64, ParseError> {
    if s == "now" {
        return Ok(now_us);
    }
    if let Some(rest) = s.strip_prefix('-') {
        // negative ts = relative to now, in µs (e.g. -1000 = 1000 µs ago)
        let v: i64 = rest.parse().map_err(|_| ParseError(format!("bad ts '{s}'")))?;
        return Ok(now_us - v);
    }
    if let Some(rest) = s.strip_prefix('+') {
        let v: i64 = rest.parse().map_err(|_| ParseError(format!("bad ts '{s}'")))?;
        return Ok(now_us + v);
    }
    s.parse::<i64>().map_err(|_| ParseError(format!("bad ts '{s}'")))
}

fn parse_loc(s: &str) -> Result<i64, ParseError> {
    if s.contains(':') {
        let mut it = s.splitn(2, ':');
        let lat: f64 = it.next().unwrap().parse().map_err(|_| ParseError(format!("bad lat '{s}'")))?;
        let lon: f64 = it.next().unwrap().parse().map_err(|_| ParseError(format!("bad lon '{s}'")))?;
        Ok(to_geo_xp_point(lat, lon))
    } else {
        s.parse::<i64>().map_err(|_| ParseError(format!("bad location '{s}'")))
    }
}

fn parse_elev(s: &str) -> Result<i64, ParseError> {
    // elevation is stored as a long (meters)
    if let Ok(v) = s.parse::<i64>() {
        Ok(v)
    } else {
        let v: f64 = s.parse().map_err(|_| ParseError(format!("bad elevation '{s}'")))?;
        Ok(v as i64)
    }
}

fn parse_value(s: &str) -> Result<Value, ParseError> {
    let t = s.trim();
    if t.is_empty() {
        return Err(ParseError("empty value".into()));
    }
    match t {
        "T" | "t" | "true" | "TRUE" => return Ok(Value::Bool(true)),
        "F" | "f" | "false" | "FALSE" => return Ok(Value::Bool(false)),
        "null" | "NULL" => return Ok(Value::Delete),
        _ => {}
    }
    let b = t.as_bytes();
    if b.len() >= 2 && (b[0] == b'\'' || b[0] == b'"') && b[b.len() - 1] == b[0] {
        return Ok(Value::Str(unquote(t)?));
    }
    if let Ok(v) = t.parse::<i64>() {
        return Ok(Value::Long(v));
    }
    if t.contains(['.', 'e', 'E']) || t.eq_ignore_ascii_case("nan") || t.eq_ignore_ascii_case("infinity") {
        if let Ok(v) = t.parse::<f64>() {
            return Ok(Value::Double(v));
        }
    }
    Ok(Value::Str(t.to_string()))
}

/// Parse one `/api/v0/update` line.
pub fn parse_line(line: &str, now_us: i64) -> Result<Datum, ParseError> {
    let line = line.trim_end_matches(['\r', '\n']);
    if line.trim().is_empty() {
        return Err(ParseError("empty line".into()));
    }
    let sp = line.find(' ').ok_or_else(|| ParseError("missing space before class".into()))?;
    let head = &line[..sp];
    let rest = line[sp + 1..].trim_start();

    let mut parts = head.splitn(3, '/');
    let ts = parse_ts(parts.next().unwrap(), now_us)?;
    let location = match parts.next() {
        Some("") | None => NO_LOCATION,
        Some(l) => parse_loc(l)?,
    };
    let elevation = match parts.next() {
        Some("") | None => NO_ELEVATION,
        Some(e) => parse_elev(e)?,
    };

    // class{labels} value
    let (class, labels, value_str) = if let Some(ob) = rest.find('{') {
        let class = rest[..ob].to_string();
        if class.is_empty() {
            return Err(ParseError("empty class".into()));
        }
        // find matching closing brace (respect quotes)
        let bytes = rest.as_bytes();
        let mut in_quote: Option<u8> = None;
        let mut cb = None;
        for (i, &c) in bytes.iter().enumerate().skip(ob + 1) {
            match in_quote {
                Some(q) => {
                    if c == q {
                        in_quote = None;
                    }
                }
                None => {
                    if c == b'\'' || c == b'"' {
                        in_quote = Some(c);
                    } else if c == b'}' {
                        cb = Some(i);
                        break;
                    }
                }
            }
        }
        let cb = cb.ok_or_else(|| ParseError("unbalanced '{'".into()))?;
        let labels = parse_labels(&rest[ob + 1..cb])?;
        let value_str = rest[cb + 1..].trim();
        (class, labels, value_str)
    } else {
        let sp2 = rest.find(' ').ok_or_else(|| ParseError("missing value".into()))?;
        (rest[..sp2].to_string(), BTreeMap::new(), rest[sp2 + 1..].trim())
    };

    let value = parse_value(value_str)?;
    Ok(Datum {
        ts,
        location,
        elevation,
        class,
        labels,
        value,
    })
}
