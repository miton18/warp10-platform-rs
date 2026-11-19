//! Minimal Apache Thrift **compact protocol** writer for the two structs the
//! ingress publishes on Kafka (see `warp10/src/main/thrift`):
//! - `io.warp10.continuum.store.thrift.data.KafkaDataMessage`
//! - `io.warp10.continuum.store.thrift.data.Metadata`
//!
//! The platform serializes with `TCompactProtocol` (ThriftUtils.getTSerializer).
//! Golden vectors generated with the reference Python thrift library — see
//! `tests/vectors.rs`.

use std::collections::BTreeMap;

// compact protocol wire types
const T_STOP: u8 = 0x00;
const T_I64: u8 = 0x06;
const T_BINARY: u8 = 0x08;
const T_LIST: u8 = 0x09;
const T_MAP: u8 = 0x0b;
const T_STRUCT: u8 = 0x0c;
const T_I32: u8 = 0x05;

pub struct CompactWriter {
    pub buf: Vec<u8>,
    last_field_id: i16,
    // stack of last field ids for nested structs
    stack: Vec<i16>,
}

impl CompactWriter {
    pub fn new() -> Self {
        CompactWriter { buf: Vec::new(), last_field_id: 0, stack: Vec::new() }
    }

    fn write_field_header(&mut self, fid: i16, ttype: u8) {
        let delta = fid - self.last_field_id;
        if (1..=15).contains(&delta) {
            self.buf.push(((delta as u8) << 4) | ttype);
        } else {
            self.buf.push(ttype);
            self.write_zigzag_varint(fid as i64);
        }
        self.last_field_id = fid;
    }

    fn write_zigzag_varint(&mut self, v: i64) {
        self.write_uvarint(((v << 1) ^ (v >> 63)) as u64);
    }

    fn write_uvarint(&mut self, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                self.buf.push(b | 0x80);
            } else {
                self.buf.push(b);
                break;
            }
        }
    }

    pub fn struct_begin(&mut self) {
        self.stack.push(self.last_field_id);
        self.last_field_id = 0;
    }

    pub fn struct_end(&mut self) {
        self.buf.push(T_STOP);
        self.last_field_id = self.stack.pop().unwrap_or(0);
    }

    pub fn i64_field(&mut self, fid: i16, v: i64) {
        self.write_field_header(fid, T_I64);
        self.write_zigzag_varint(v);
    }

    pub fn i32_field(&mut self, fid: i16, v: i32) {
        self.write_field_header(fid, T_I32);
        self.write_zigzag_varint(v as i64);
    }

    pub fn binary_field(&mut self, fid: i16, v: &[u8]) {
        self.write_field_header(fid, T_BINARY);
        self.write_uvarint(v.len() as u64);
        self.buf.extend_from_slice(v);
    }

    pub fn string_map_field(&mut self, fid: i16, map: &BTreeMap<String, String>) {
        self.write_field_header(fid, T_MAP);
        let n = map.len();
        if n <= 14 {
            self.buf.push(n as u8);
        } else {
            self.buf.push(0x00);
            self.write_uvarint(n as u64);
        }
        self.buf.push((T_BINARY << 4) | T_BINARY);
        for (k, v) in map {
            self.write_uvarint(k.len() as u64);
            self.buf.extend_from_slice(k.as_bytes());
            self.write_uvarint(v.len() as u64);
            self.buf.extend_from_slice(v.as_bytes());
        }
    }

    pub fn struct_field_begin(&mut self, fid: i16) {
        self.write_field_header(fid, T_STRUCT);
    }

    pub fn list_i64_field(&mut self, fid: i16, vals: &[i64]) {
        self.write_field_header(fid, T_LIST);
        let n = vals.len();
        if n <= 14 {
            self.buf.push(((n as u8) << 4) | T_I64);
        } else {
            self.buf.push(0xf0 | T_I64);
            self.write_uvarint(n as u64);
        }
        for v in vals {
            self.write_zigzag_varint(*v);
        }
    }
}

#[derive(Clone, Copy)]
pub enum KafkaDataMessageType {
    Store = 1,
    Archive = 2,
    Delete = 3,
}

pub struct KafkaDataMessage<'a> {
    pub class_id: i64,
    pub labels_id: i64,
    pub data: Option<&'a [u8]>,
    pub indices: Option<&'a [i64]>,
    pub deletion_start: Option<i64>,
    pub deletion_end: Option<i64>,
    pub msg_type: KafkaDataMessageType,
    pub deletion_min_age: Option<i64>,
    pub metadata: Option<&'a Metadata<'a>>,
    pub attributes: Option<&'a BTreeMap<String, String>>,
}

impl<'a> KafkaDataMessage<'a> {
    pub fn serialize(&self) -> Vec<u8> {
        let mut w = CompactWriter::new();
        w.struct_begin();
        w.i64_field(1, self.class_id);
        w.i64_field(2, self.labels_id);
        if let Some(d) = self.data {
            w.binary_field(3, d);
        }
        if let Some(ix) = self.indices {
            w.list_i64_field(4, ix);
        }
        if let Some(v) = self.deletion_start {
            w.i64_field(5, v);
        }
        if let Some(v) = self.deletion_end {
            w.i64_field(6, v);
        }
        w.i32_field(7, self.msg_type as i32);
        if let Some(v) = self.deletion_min_age {
            w.i64_field(8, v);
        }
        if let Some(m) = self.metadata {
            w.struct_field_begin(9);
            w.buf.extend_from_slice(&m.serialize());
        }
        if let Some(attrs) = self.attributes {
            w.string_map_field(10, attrs);
        }
        w.struct_end();
        w.buf
    }
}

pub struct Metadata<'a> {
    pub name: &'a str,
    pub labels: &'a BTreeMap<String, String>,
    pub class_id: i64,
    pub labels_id: i64,
    pub attributes: Option<&'a BTreeMap<String, String>>,
    pub source: Option<&'a str>,
    pub last_activity: Option<i64>,
}

impl<'a> Metadata<'a> {
    pub fn serialize(&self) -> Vec<u8> {
        let mut w = CompactWriter::new();
        w.struct_begin();
        w.binary_field(1, self.name.as_bytes());
        w.string_map_field(2, self.labels);
        w.i64_field(3, self.class_id);
        w.i64_field(4, self.labels_id);
        if let Some(a) = self.attributes {
            w.string_map_field(5, a);
        }
        if let Some(s) = self.source {
            w.binary_field(6, s.as_bytes());
        }
        if let Some(v) = self.last_activity {
            w.i64_field(7, v);
        }
        w.struct_end();
        w.buf
    }
}
