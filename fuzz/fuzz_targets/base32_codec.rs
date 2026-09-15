#![no_main]

use libfuzzer_sys::fuzz_target;
use sotto_core::format;

const MAX_INPUT: usize = 16384;
const ALPHABET: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

fn bounded(data: &[u8]) -> &[u8] {
    let end = data.len().min(MAX_INPUT);
    &data[..end]
}

fn structured_ascii(data: &[u8]) -> String {
    data.iter()
        .map(|byte| match byte % 40 {
            value if value < 32 => ALPHABET[value as usize] as char,
            32 => '-',
            33 => 'o',
            34 => 'i',
            35 => 'l',
            36 => 'U',
            _ => '#',
        })
        .collect()
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        let _ = format::decode("");
        return;
    };
    let input = bounded(rest);
    match mode % 3 {
        0 => {
            let encoded = format::encode(input);
            assert_eq!(format::decode(&encoded).expect("encoder output is valid"), input);
        }
        1 => {
            if let Ok(text) = std::str::from_utf8(input) {
                let _ = format::decode(text);
            }
        }
        _ => {
            let mut text = structured_ascii(input);
            text.push('#');
            assert!(format::decode(&text).is_err(), "a deliberately invalid symbol must reject");
        }
    }
});
