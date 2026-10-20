//! A minimal, deterministic PNG encoder for the `images` timeline.
//!
//! Only 8-bit RGB with no ancillary chunks is written, so the same pixels
//! always produce the same bytes regardless of platform or library version
//! (the zlib stream comes from the pinned `flate2` backend).

use std::io::Write as _;

use flate2::Compression;
use flate2::write::ZlibEncoder;

/// Encode `width × height` RGB pixels (`rgb.len() == width * height * 3`).
pub fn encode_rgb(width: u32, height: u32, rgb: &[u8]) -> Vec<u8> {
    assert_eq!(rgb.len(), width as usize * height as usize * 3);
    let stride = width as usize * 3;
    let mut raw = Vec::with_capacity((stride + 1) * height as usize);
    for row in rgb.chunks(stride) {
        // Filter type 0 (None) per scanline keeps the encoder trivial.
        raw.push(0);
        raw.extend_from_slice(row);
    }
    let mut zlib = ZlibEncoder::new(Vec::new(), Compression::new(6));
    zlib.write_all(&raw).expect("writing to a Vec cannot fail");
    let idat = zlib.finish().expect("writing to a Vec cannot fail");

    let mut png = b"\x89PNG\r\n\x1a\n".to_vec();
    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend_from_slice(&width.to_be_bytes());
    ihdr.extend_from_slice(&height.to_be_bytes());
    // Bit depth 8, colour type 2 (RGB), deflate, adaptive filtering, no interlace.
    ihdr.extend_from_slice(&[8, 2, 0, 0, 0]);
    write_chunk(&mut png, b"IHDR", &ihdr);
    write_chunk(&mut png, b"IDAT", &idat);
    write_chunk(&mut png, b"IEND", &[]);
    png
}

fn write_chunk(out: &mut Vec<u8>, kind: &[u8; 4], data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(kind);
    out.extend_from_slice(data);
    let mut crc = flate2::Crc::new();
    crc.update(kind);
    crc.update(data);
    out.extend_from_slice(&crc.sum().to_be_bytes());
}

/// A recognizable test card: a diagonal gradient whose hue depends on
/// `index`, with a checker band and a frame, so every image differs and
/// decoding cost resembles a real screenshot more than a flat fill would.
pub fn test_card(index: u32, width: u32, height: u32) -> Vec<u8> {
    let mut rgb = Vec::with_capacity(width as usize * height as usize * 3);
    let base = [
        (index * 53 % 256) as u8,
        (index * 97 % 256) as u8,
        (index * 151 % 256) as u8,
    ];
    for y in 0..height {
        for x in 0..width {
            let frame = x < 4 || y < 4 || x >= width - 4 || y >= height - 4;
            let checker = y > height / 3 && y < height / 3 + 24 && ((x / 12 + y / 12) % 2 == 0);
            let pixel = if frame {
                [24, 24, 28]
            } else if checker {
                [240, 240, 240]
            } else {
                let t = ((x + y) * 255 / (width + height)) as u8;
                [
                    base[0].wrapping_add(t),
                    base[1].wrapping_add(t / 2),
                    base[2].wrapping_sub(t / 3),
                ]
            };
            rgb.extend_from_slice(&pixel);
        }
    }
    encode_rgb(width, height, &rgb)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn png_has_signature_and_is_deterministic() {
        let a = test_card(3, 64, 48);
        let b = test_card(3, 64, 48);
        assert_eq!(a, b);
        assert_eq!(&a[..8], b"\x89PNG\r\n\x1a\n");
        assert_eq!(&a[12..16], b"IHDR");
        assert_eq!(&a[a.len() - 8..a.len() - 4], b"IEND");
        assert_ne!(a, test_card(4, 64, 48));
    }

    #[test]
    fn chunk_crc_matches_the_png_reference_for_iend() {
        let png = encode_rgb(1, 1, &[0, 0, 0]);
        // IEND's CRC is the constant AE 42 60 82 in every PNG.
        assert_eq!(&png[png.len() - 4..], &[0xae, 0x42, 0x60, 0x82]);
    }
}
