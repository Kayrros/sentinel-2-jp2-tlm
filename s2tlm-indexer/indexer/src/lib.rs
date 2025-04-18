use anyhow::Context;
use anyhow::Result;
use opendal::Reader;

use reader::CachedReader;

pub mod manifest;
mod reader;

/// JPEG 2000 signature box
const JP2_JP: u32 = 0x6a502020;
/// Contiguous codestream box
const JP2_JP2C: u32 = 0x6a703263;

/// SOC marker value
const J2K_MS_SOC: u16 = 0xff4f;
/// TLM marker value
const J2K_MS_TLM: u16 = 0xff55;
/// SOT marker value
const J2K_MS_SOT: u16 = 0xff90;
/// SOD marker value
const J2K_MS_SOD: u16 = 0xff93;
/// EOC marker value
const J2K_MS_EOC: u16 = 0xffd9;

#[derive(Debug)]
struct BoxHeader {
    boxtype: u32,
    data_start: usize,
}

impl BoxHeader {
    async fn read(reader: &mut CachedReader, cur: &mut usize) -> Result<Self> {
        let s = reader.read(*cur..*cur + 16).await?;
        anyhow::ensure!(s.len() >= 8);

        let boxtype = u32::from_be_bytes(s[4..8].try_into().unwrap());

        let mut length: usize = u32::from_be_bytes(s[0..4].try_into().unwrap()) as _;
        let data_start = if length == 1 {
            anyhow::ensure!(s.len() >= 16);
            length = u64::from_be_bytes(s[8..16].try_into().unwrap()) as _;
            *cur + 16
        } else {
            *cur + 8
        };

        *cur += length;

        Ok(BoxHeader {
            boxtype,
            data_start,
        })
    }
}

#[derive(Debug, Clone)]
struct MarkerHeader {
    code: u16,
    data_start: usize,
    data_length: usize,
}

impl MarkerHeader {
    async fn read(s: &mut CachedReader, cur: &mut usize) -> Result<Self> {
        let s = s.read(*cur..*cur + 4).await?;
        anyhow::ensure!(s.len() >= 2);

        let code = u16::from_be_bytes(s[0..2].try_into().unwrap());

        let (data_length, data_start) =
            if code == J2K_MS_SOC || code == J2K_MS_SOD || code == J2K_MS_EOC {
                (0, *cur + 2)
            } else {
                anyhow::ensure!(s.len() >= 4);
                // does not include the tile marker
                let length = u16::from_be_bytes(s[2..4].try_into().unwrap()) as usize;
                (length - 2, *cur + 4)
            };

        *cur = data_start + data_length;

        Ok(MarkerHeader {
            code,
            data_start,
            data_length,
        })
    }
}

#[derive(Debug)]
struct SOTMarker {
    isot: u16,
    psot: u32,
    tpsot: u8,
    tnsot: u8,
}

impl SOTMarker {
    fn from_marker(s: &[u8; 8]) -> Self {
        let isot = u16::from_be_bytes(s[0..2].try_into().unwrap());
        let psot = u32::from_be_bytes(s[2..6].try_into().unwrap());
        let tpsot = s[6];
        let tnsot = s[7];

        SOTMarker {
            isot,
            psot,
            tpsot,
            tnsot,
        }
    }
}

#[derive(Debug)]
struct JP2FileMetadata {
    file_length: u64,
    position_first_sot: u64,
}

/// The index is composed of some metadata and the TLM marker.
/// Metadata is:
/// - u64: position of the end of main header of the original file
/// - u64: size of the original file
///
/// For the original TLM marker spec, see https://ics.uci.edu/~dhirschb/class/267/papers/jpeg2000.pdf table A-33
fn generate_tlmindex(file_metadata: &JP2FileMetadata, mut tile_entries: Vec<SOTMarker>) -> Vec<u8> {
    tile_entries.sort_by_key(|t| t.isot); // also tpsot / tnsot

    let mut per_tile_length = 0;

    #[allow(non_snake_case)]
    let ST = {
        if let Some(t) = tile_entries.last() {
            if t.isot <= 254 {
                per_tile_length += 1;
                1 // u8
            } else {
                per_tile_length += 2;
                2 // u16
            }
        } else {
            0
        }
    };

    #[allow(non_snake_case)]
    let SP = {
        per_tile_length += 2;
        let mut sp = 0; // u16
        for t in &tile_entries {
            if t.psot > 65_534 {
                per_tile_length += 2;
                sp = 1; // u32
                break;
            }
        }
        sp
    };

    #[allow(non_snake_case)]
    let (Stlm, per_tile_length) = {
        let mut stlm = 0;

        if ST == 1 {
            stlm |= 0b00010000;
        } else if ST == 2 {
            stlm |= 0b00100000;
        }

        if SP == 1 {
            stlm |= 0b01000000;
        }

        (stlm, per_tile_length)
    };

    log::debug!("ST={} SP={}", ST, SP);

    let header_segment_length = 2 /* Ltlm */ + 1 /* Ztlm */ + 1 /* Stlm */;
    let segment_length = header_segment_length + tile_entries.len() * per_tile_length;
    let metadata_length = 8 /* file length */ + 8 /* position first SOT */ + 4 /* full tlm segment length */;
    let full_tlm_segment_length = 2 /* marker */ + segment_length;

    let mut out = Vec::<u8>::with_capacity(metadata_length + full_tlm_segment_length);

    out.extend_from_slice(&file_metadata.file_length.to_be_bytes());
    out.extend_from_slice(&file_metadata.position_first_sot.to_be_bytes());
    out.extend_from_slice(&(full_tlm_segment_length as u32).to_be_bytes());

    out.extend_from_slice(&J2K_MS_TLM.to_be_bytes());
    out.extend_from_slice(&(segment_length as u16).to_be_bytes()); // Ttlm
    out.push(0); // Ztlm
    out.push(Stlm);

    for sot in tile_entries {
        if ST == 1 {
            out.extend_from_slice(&u8::to_be_bytes(
                // try_into+unwrap is OK because we wouldn't be with ST==1 otherwise
                sot.isot.try_into().unwrap(),
            ));
        } else if ST == 2 {
            out.extend_from_slice(&u16::to_be_bytes(sot.isot));
        }
        if SP == 0 {
            out.extend_from_slice(&u16::to_be_bytes(
                // try_into+unwrap is OK because we wouldn't be with SP==0 otherwise
                sot.psot.try_into().unwrap(),
            ));
        } else if SP == 1 {
            out.extend_from_slice(&u32::to_be_bytes(sot.psot));
        }
    }

    assert!(out.len() == metadata_length + full_tlm_segment_length);

    out
}

pub async fn make_index(reader: Reader, length: usize) -> anyhow::Result<Vec<u8>> {
    let mut reader = CachedReader::from(reader, length).await?;

    let mut cur = 0;
    let mut jbox = BoxHeader::read(&mut reader, &mut cur).await?;
    assert!(jbox.boxtype == JP2_JP);

    // look for the jp2 codestream
    while jbox.boxtype != JP2_JP2C {
        jbox = BoxHeader::read(&mut reader, &mut cur).await?;
    }

    // enter in the codestream
    let mut cur = jbox.data_start;
    let mut marker = MarkerHeader::read(&mut reader, &mut cur).await?;
    assert!(marker.code == J2K_MS_SOC);

    // look for the first SOT marker, skipping the other ones (SIZ, etc)
    while marker.code != J2K_MS_SOT {
        anyhow::ensure!(marker.code != J2K_MS_TLM, "file already has a TLM marker");
        if marker.code == J2K_MS_TLM {
            return Ok(vec![]);
        }

        marker = MarkerHeader::read(&mut reader, &mut cur).await?;
    }

    let position_first_sot = (marker.data_start - 4) as u64;

    // Sentinel-2 10m bands have 121 tiles (except TCI which has many more)
    let mut tile_entries = Vec::<_>::with_capacity(121);

    // iterate through all tiles until the end of codestream
    while marker.code != J2K_MS_EOC {
        assert!(marker.code == J2K_MS_SOT);

        // read the SOT marker
        let i = marker.data_start;
        let j = marker.data_start + 8;
        let s = reader
            .read(i..j)
            .await?
            .first_chunk()
            .context("incomplete SOT read")?;
        let sot = SOTMarker::from_marker(s);

        // multi-parts per tile is not supported (because not tested)
        anyhow::ensure!(sot.tpsot == 0, "only TpSOT=0 is supported");
        anyhow::ensure!(sot.tnsot == 1, "only TnSOT=1 is supported");

        // jump to the next SOT, skipping the SOD
        let sot_marker_length = 4;
        cur += sot.psot as usize - marker.data_length - sot_marker_length;
        marker = MarkerHeader::read(&mut reader, &mut cur).await?;

        // keep the entry
        tile_entries.push(sot);
    }

    let file_metadata = JP2FileMetadata {
        file_length: length as u64,
        position_first_sot,
    };

    // generate the TLM index
    let tlm = generate_tlmindex(&file_metadata, tile_entries);
    Ok(tlm)
}
