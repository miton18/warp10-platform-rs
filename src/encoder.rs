//! GTSEncoder binary format, ported from
//! `io.warp10.continuum.gts.GTSEncoder` (senx/warp10-platform).
//!
//! The stream contains only datums (the base timestamp is kept out-of-band;
//! the ingress uses base 0 and the Store decodes with `new GTSDecoder(0L, ..)`).
//! No AES wrapping (wrappingKey = null) — `getBytes` returns the raw stream.
//!
//! Validated byte-for-byte against the upstream Java class — see
//! `tests/vectors.rs`.

use crate::geoxp::{NO_ELEVATION, NO_LOCATION};
use crate::gts::Value;

// Flags (GTSEncoder.java)
const FLAGS_MASK_CONTINUATION: u8 = 0x80;
const FLAGS_MASK_TIMESTAMP: u8 = 0x60;
const FLAGS_MASK_TYPE: u8 = 0x18;

const FLAGS_CONTINUATION: u8 = 0x80;
const FLAGS_TIMESTAMP_ZIGZAG_DELTA_PREVIOUS: u8 = 0x00;
const FLAGS_TIMESTAMP_EQUALS_BASE: u8 = 0x20;
const FLAGS_TIMESTAMP_ZIGZAG_DELTA_BASE: u8 = 0x40;
const FLAGS_TIMESTAMP_RAW_ABSOLUTE: u8 = 0x60;

const FLAGS_TYPE_BOOLEAN: u8 = 0x00;
const FLAGS_TYPE_LONG: u8 = 0x08;
const FLAGS_TYPE_DOUBLE: u8 = 0x10;
const FLAGS_TYPE_STRING: u8 = 0x18;

const FLAGS_BOOLEAN_VALUE_TRUE: u8 = 0x04;
const FLAGS_BOOLEAN_VALUE_FALSE: u8 = 0x02;
const FLAGS_DELETE_MARKER: u8 = 0x07;
const FLAGS_LONG_ZIGZAG: u8 = 0x04;
const FLAGS_LONG_DELTA_PREVIOUS: u8 = 0x02;
const FLAGS_VALUE_IDENTICAL: u8 = 0x01;
const FLAGS_DOUBLE_IEEE754: u8 = 0x04;

const FLAGS_LOCATION: u8 = 0x40;
const FLAGS_LOCATION_GEOXPPOINT_ZIGZAG_DELTA: u8 = 0x20;
const FLAGS_LOCATION_IDENTICAL: u8 = 0x10;
const FLAGS_ELEVATION: u8 = 0x08;
const FLAGS_ELEVATION_ZIGZAG: u8 = 0x04;
const FLAGS_ELEVATION_DELTA_PREVIOUS: u8 = 0x02;
const FLAGS_ELEVATION_IDENTICAL: u8 = 0x01;

pub fn encode_unsigned_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let b = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            buf.push(b | 0x80);
        } else {
            buf.push(b);
            break;
        }
    }
}

pub fn encode_signed_varint(value: i64, buf: &mut Vec<u8>) {
    let zz = ((value << 1) ^ (value >> 63)) as u64;
    encode_unsigned_varint(zz, buf);
}

pub struct Encoder {
    pub base: i64,
    last_ts: i64,
    last_loc: i64,
    last_elev: i64,
    last_long: i64,
    last_double: f64,
    /// stripped (unscaled, scale) of the last BigDecimal-encoded value
    last_bd: Option<(i128, i32)>,
    last_string: String,
    valid_long: bool,
    valid_double: bool,
    valid_string: bool,
    valid_loc: bool,
    valid_elev: bool,
    buf: Vec<u8>,
    pub count: u64,
}

impl Encoder {
    /// New encoder. The platform ingress uses base 0 (see GTSHelper.parse /
    /// Store's GTSDecoder(0L, ..)).
    pub fn new(base: i64) -> Self {
        Encoder {
            base,
            last_ts: 0,
            last_loc: NO_LOCATION,
            last_elev: NO_ELEVATION,
            last_long: i64::MAX,
            last_double: 0.0,
            last_bd: None,
            last_string: String::new(),
            valid_long: false,
            valid_double: false,
            valid_string: false,
            valid_loc: false,
            valid_elev: false,
            buf: Vec::new(),
            count: 0,
        }
    }

    pub fn get_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub fn add_value(&mut self, ts: i64, location: i64, elevation: i64, value: &Value) {
        let mut ts_type_flag: u8 = 0x0;

        // --- timestamp flag ---
        if self.base == ts {
            ts_type_flag |= FLAGS_TIMESTAMP_EQUALS_BASE;
        } else if self.last_ts != 0 {
            let delta_base = (ts - self.base).abs();
            let delta_last = (ts - self.last_ts).abs();
            if delta_base < delta_last {
                if delta_base < (1i64 << 48) {
                    ts_type_flag |= FLAGS_TIMESTAMP_ZIGZAG_DELTA_BASE;
                } else {
                    ts_type_flag |= FLAGS_TIMESTAMP_RAW_ABSOLUTE;
                }
            } else if delta_last < (1i64 << 48) {
                ts_type_flag |= FLAGS_TIMESTAMP_ZIGZAG_DELTA_PREVIOUS;
            } else {
                ts_type_flag |= FLAGS_TIMESTAMP_RAW_ABSOLUTE;
            }
        } else if (ts - self.base).abs() < (1i64 << 48) {
            ts_type_flag |= FLAGS_TIMESTAMP_ZIGZAG_DELTA_BASE;
        } else {
            ts_type_flag |= FLAGS_TIMESTAMP_RAW_ABSOLUTE;
        }

        // --- value type flags ---
        let mut loc_elev_flag: u8 = 0x0;
        match value {
            Value::Long(lv) => {
                ts_type_flag |= FLAGS_TYPE_LONG;
                if self.valid_long && *lv == self.last_long {
                    ts_type_flag |= FLAGS_VALUE_IDENTICAL;
                } else {
                    let offset = lv.wrapping_sub(self.last_long);
                    if self.valid_long
                        && offset.abs() < lv.abs()
                        && offset.abs() < (1i64 << 48)
                    {
                        ts_type_flag |= FLAGS_LONG_DELTA_PREVIOUS;
                        ts_type_flag |= FLAGS_LONG_ZIGZAG;
                    } else if lv.abs() < (1i64 << 48) {
                        ts_type_flag |= FLAGS_LONG_ZIGZAG;
                    }
                }
            }
            Value::Double(dv) => {
                ts_type_flag |= FLAGS_TYPE_DOUBLE;
                if self.valid_double && (*dv == self.last_double || (dv.is_nan() && self.last_double.is_nan())) {
                    ts_type_flag |= FLAGS_VALUE_IDENTICAL;
                } else {
                    ts_type_flag |= FLAGS_DOUBLE_IEEE754;
                }
            }
            Value::Decimal { unscaled, scale } => {
                // Mirrors the BigDecimal branch of GTSEncoder.addValue: strip
                // trailing zeros, then IDENTICAL if numerically equal to the
                // last BigDecimal, else IEEE754 when scale or mantissa do not
                // fit, else the compact scale+unscaled encoding.
                ts_type_flag |= FLAGS_TYPE_DOUBLE;
                let (u, sc) = strip_trailing_zeros(*unscaled, *scale);
                let identical = self.last_bd == Some((u, sc));
                if identical {
                    ts_type_flag |= FLAGS_VALUE_IDENTICAL;
                } else if sc > 127 || sc < -128 || bigint_bitlen(u) > 46 {
                    ts_type_flag |= FLAGS_DOUBLE_IEEE754;
                }
            }
            Value::Str(s) => {
                ts_type_flag |= FLAGS_TYPE_STRING;
                if self.valid_string && *s == self.last_string {
                    ts_type_flag |= FLAGS_VALUE_IDENTICAL;
                }
            }
            Value::Bool(b) => {
                ts_type_flag |= FLAGS_TYPE_BOOLEAN;
                ts_type_flag |= if *b { FLAGS_BOOLEAN_VALUE_TRUE } else { FLAGS_BOOLEAN_VALUE_FALSE };
            }
            Value::Delete => {
                ts_type_flag |= FLAGS_TYPE_BOOLEAN;
                ts_type_flag |= FLAGS_DELETE_MARKER;
            }
        }

        // --- location / elevation flags ---
        if location != NO_LOCATION && !matches!(value, Value::Delete) {
            ts_type_flag |= FLAGS_CONTINUATION;
            loc_elev_flag |= FLAGS_LOCATION;
            if self.valid_loc {
                if self.last_loc == location {
                    loc_elev_flag |= FLAGS_LOCATION_IDENTICAL;
                } else if (location - self.last_loc).abs() < (1i64 << 48) {
                    loc_elev_flag |= FLAGS_LOCATION_GEOXPPOINT_ZIGZAG_DELTA;
                }
            }
        } else {
            self.valid_loc = false;
        }
        if elevation != NO_ELEVATION && !matches!(value, Value::Delete) {
            ts_type_flag |= FLAGS_CONTINUATION;
            loc_elev_flag |= FLAGS_ELEVATION;
            if self.valid_elev {
                if self.last_elev == elevation {
                    loc_elev_flag |= FLAGS_ELEVATION_IDENTICAL;
                } else if (elevation - self.last_elev).abs() < (1i64 << 48) {
                    loc_elev_flag |= FLAGS_ELEVATION_DELTA_PREVIOUS;
                    loc_elev_flag |= FLAGS_ELEVATION_ZIGZAG;
                } else if elevation.abs() < (1i64 << 48) {
                    loc_elev_flag |= FLAGS_ELEVATION_ZIGZAG;
                }
            } else if elevation.abs() < (1i64 << 48) {
                loc_elev_flag |= FLAGS_ELEVATION_ZIGZAG;
            }
        } else {
            self.valid_elev = false;
        }

        // --- write ---
        self.buf.push(ts_type_flag);
        if ts_type_flag & FLAGS_MASK_CONTINUATION == FLAGS_CONTINUATION {
            self.buf.push(loc_elev_flag);
        }
        match ts_type_flag & FLAGS_MASK_TIMESTAMP {
            FLAGS_TIMESTAMP_RAW_ABSOLUTE => self.buf.extend_from_slice(&ts.to_be_bytes()),
            FLAGS_TIMESTAMP_EQUALS_BASE => {}
            FLAGS_TIMESTAMP_ZIGZAG_DELTA_BASE => encode_signed_varint(ts - self.base, &mut self.buf),
            FLAGS_TIMESTAMP_ZIGZAG_DELTA_PREVIOUS => {
                encode_signed_varint(ts - self.last_ts, &mut self.buf)
            }
            _ => unreachable!(),
        }
        self.last_ts = ts;

        if loc_elev_flag & FLAGS_LOCATION == FLAGS_LOCATION {
            if loc_elev_flag & FLAGS_LOCATION_IDENTICAL != FLAGS_LOCATION_IDENTICAL {
                if loc_elev_flag & FLAGS_LOCATION_GEOXPPOINT_ZIGZAG_DELTA
                    == FLAGS_LOCATION_GEOXPPOINT_ZIGZAG_DELTA
                {
                    encode_signed_varint(location - self.last_loc, &mut self.buf);
                } else {
                    self.buf.extend_from_slice(&location.to_be_bytes());
                }
            }
            self.last_loc = location;
            self.valid_loc = true;
        }
        if loc_elev_flag & FLAGS_ELEVATION == FLAGS_ELEVATION {
            if loc_elev_flag & FLAGS_ELEVATION_IDENTICAL != FLAGS_ELEVATION_IDENTICAL {
                let zigzag = loc_elev_flag & FLAGS_ELEVATION_ZIGZAG == FLAGS_ELEVATION_ZIGZAG;
                let mut toencode = elevation;
                if loc_elev_flag & FLAGS_ELEVATION_DELTA_PREVIOUS == FLAGS_ELEVATION_DELTA_PREVIOUS {
                    toencode = elevation - self.last_elev;
                }
                if zigzag {
                    encode_signed_varint(toencode, &mut self.buf);
                } else {
                    self.buf.extend_from_slice(&toencode.to_be_bytes());
                }
            }
            self.last_elev = elevation;
            self.valid_elev = true;
        }

        match ts_type_flag & FLAGS_MASK_TYPE {
            FLAGS_TYPE_STRING => {
                if ts_type_flag & FLAGS_VALUE_IDENTICAL != FLAGS_VALUE_IDENTICAL {
                    if let Value::Str(s) = value {
                        let utf8 = s.as_bytes();
                        encode_unsigned_varint(utf8.len() as u64, &mut self.buf);
                        self.buf.extend_from_slice(utf8);
                        self.last_string = s.clone();
                        self.valid_string = true;
                    }
                }
            }
            FLAGS_TYPE_LONG => {
                if ts_type_flag & FLAGS_VALUE_IDENTICAL != FLAGS_VALUE_IDENTICAL {
                    if let Value::Long(lv) = value {
                        let mut toencode = *lv;
                        if ts_type_flag & FLAGS_LONG_DELTA_PREVIOUS == FLAGS_LONG_DELTA_PREVIOUS {
                            toencode = lv.wrapping_sub(self.last_long);
                        }
                        if ts_type_flag & FLAGS_LONG_ZIGZAG == FLAGS_LONG_ZIGZAG {
                            encode_signed_varint(toencode, &mut self.buf);
                        } else {
                            self.buf.extend_from_slice(&toencode.to_be_bytes());
                        }
                        self.last_long = *lv;
                        self.valid_long = true;
                    }
                }
            }
            FLAGS_TYPE_DOUBLE => {
                if ts_type_flag & FLAGS_VALUE_IDENTICAL != FLAGS_VALUE_IDENTICAL {
                    match value {
                        Value::Double(dv) => {
                            self.buf.extend_from_slice(&dv.to_be_bytes());
                            self.last_double = *dv;
                            self.valid_double = true;
                            self.last_bd = None;
                        }
                        Value::Decimal { unscaled, scale } => {
                            if ts_type_flag & FLAGS_DOUBLE_IEEE754 != 0 {
                                let dv = (*unscaled as f64) * 10f64.powi(-scale);
                                self.buf.extend_from_slice(&dv.to_be_bytes());
                                self.last_double = dv;
                                self.valid_double = true;
                                self.last_bd = None;
                            } else {
                                let (u, sc) = strip_trailing_zeros(*unscaled, *scale);
                                self.buf.push(sc as u8);
                                encode_signed_varint(u as i64, &mut self.buf);
                                self.last_bd = Some((u, sc));
                                self.valid_double = false;
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
            FLAGS_TYPE_BOOLEAN => {}
            _ => unreachable!(),
        }
        self.count += 1;
    }
}

/// BigDecimal.stripTrailingZeros for non-zero values (zero normalizes to 0E0).
pub fn strip_trailing_zeros(mut unscaled: i128, mut scale: i32) -> (i128, i32) {
    if unscaled == 0 {
        return (0, 0);
    }
    while unscaled % 10 == 0 {
        unscaled /= 10;
        scale -= 1;
    }
    (unscaled, scale)
}

/// java.math.BigInteger.bitLength (two's complement length without sign bit).
fn bigint_bitlen(v: i128) -> u32 {
    let m: u128 = if v >= 0 { v as u128 } else { (!v) as u128 };
    if m == 0 {
        0
    } else {
        128 - m.leading_zeros()
    }
}
