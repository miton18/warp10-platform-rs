//! GeoXPPoint (HHCode) encoding, ported from `com.geoxp.GeoXPLib` /
//! `com.geoxp.HHCodeHelper` (senx/geoxplib), resolution 32.

pub const NO_LOCATION: i64 = 0x0145014501450145;
pub const NO_ELEVATION: i64 = i64::MIN;

const DEGREES_PER_LAT_UNIT: f64 = 180.0 / (1u64 << 32) as f64;
const DEGREES_PER_LON_UNIT: f64 = 360.0 / (1u64 << 32) as f64;

/// Interleave the bits of `lat` and `lon` into a HHCode at `resolution` bits.
fn build_hhcode(lat: i64, lon: i64, resolution: u32) -> i64 {
    let mut lat = lat;
    let mut lon = lon;
    if lat & 0x1_0000_0000 != 0 {
        lat ^= 0xffff_ffff;
        lon ^= 0x8000_0000;
    }
    lat &= 0xffff_ffff;
    lon &= 0xffff_ffff;
    let mut hhcode: i64 = 0;
    let mut i: i32 = 31;
    while i >= 32 - resolution as i32 {
        hhcode <<= 1;
        hhcode |= (lat >> i) & 1;
        hhcode <<= 1;
        hhcode |= (lon >> i) & 1;
        i -= 1;
    }
    if resolution != 32 {
        hhcode <<= 32 - resolution;
    }
    hhcode
}

/// Convert (lat, lon) in degrees to a GeoXPPoint (HHCode at max resolution).
pub fn to_geo_xp_point(lat: f64, lon: f64) -> i64 {
    let lat = lat + 90.0;
    let lon = lon + 180.0;
    let x = (lat / DEGREES_PER_LAT_UNIT).floor() as i64;
    let y = (lon / DEGREES_PER_LON_UNIT).floor() as i64;
    build_hhcode(x, y, 32)
}
