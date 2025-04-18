/// List files referenced in `manifest.safe` xmls.
use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct DataObject<'a> {
    #[serde(rename = "ID", default)]
    pub id: &'a str,

    #[serde(rename = "byteStream")]
    pub byte_stream: ByteStream<'a>,
}

#[derive(Debug, Deserialize)]
pub struct ByteStream<'a> {
    #[serde(rename = "mimeType", default)]
    pub mime_type: &'a str,

    #[serde(rename = "size", default)]
    pub size: u64,

    #[serde(rename = "fileLocation")]
    pub file_location: FileLocation<'a>,

    #[serde(rename = "checksum")]
    pub checksum: Checksum<'a>,
}

#[derive(Debug, Deserialize)]
pub struct FileLocation<'a> {
    #[serde(rename = "href", default)]
    pub href: &'a str,

    #[serde(rename = "locatorType", default)]
    pub locator_type: &'a str,
}

impl<'a> FileLocation<'a> {
    pub fn href_without_dotslash(&self) -> &'a str {
        self.href.trim_start_matches("./")
    }
}

#[derive(Debug, Deserialize)]
pub struct Checksum<'a> {
    #[serde(rename = "checksumName", default)]
    pub checksum_name: &'a str,

    #[serde(rename = "$text", default)]
    pub value: &'a str,
}

/// relative to the root of the SAFE folder
#[derive(Debug)]
pub struct L1CPaths<'a> {
    pub b01: &'a str,
    pub b02: &'a str,
    pub b03: &'a str,
    pub b04: &'a str,
    pub b05: &'a str,
    pub b06: &'a str,
    pub b07: &'a str,
    pub b08: &'a str,
    pub b8a: &'a str,
    pub b09: &'a str,
    pub b10: &'a str,
    pub b11: &'a str,
    pub b12: &'a str,
    pub tci: &'a str,
}

/// relative to the root of the SAFE folder
/// Only best-resolution rasters are considered (B04_10m and not B04_20m).
#[derive(Debug)]
pub struct L2APaths<'a> {
    /// 20m
    pub b01: &'a str,
    pub b02: &'a str,
    pub b03: &'a str,
    pub b04: &'a str,
    pub b05: &'a str,
    pub b06: &'a str,
    pub b07: &'a str,
    pub b08: &'a str,
    pub b8a: &'a str,
    /// 60m
    pub b09: &'a str,
    // b10 does not exist in L2A
    pub b11: &'a str,
    pub b12: &'a str,
    /// 10m
    pub tci: &'a str,
    /// 10m
    pub aot: &'a str,
    /// 10m
    pub wvp: &'a str,
    /// 20m
    pub scl: &'a str,
}

#[derive(Debug)]
pub enum Paths<'a> {
    L1C(L1CPaths<'a>),
    L2A(L2APaths<'a>),
}

pub struct ManifestPathsExtractor<'a> {
    doc: roxmltree::Document<'a>,
}

impl<'a> ManifestPathsExtractor<'a> {
    pub fn try_new(xml: &'a str) -> Result<Self> {
        let doc = roxmltree::Document::parse(xml)?;
        Ok(Self { doc })
    }

    pub fn extract(&'a self) -> Result<Paths<'a>> {
        let unit_type = self
            .doc
            .descendants()
            .find(|n| n.has_tag_name("contentUnit"))
            .context("cannot find contentUnit tag")?
            .attribute("unitType")
            .context("cannot find unitType attribute on contentUnit")?;

        let parse_l1c = || -> Result<L1CPaths> {
            let data = self
                .doc
                .descendants()
                .filter(|n| n.has_tag_name("dataObject"));

            let mut b01 = None;
            let mut b02 = None;
            let mut b03 = None;
            let mut b04 = None;
            let mut b05 = None;
            let mut b06 = None;
            let mut b07 = None;
            let mut b08 = None;
            let mut b8a = None;
            let mut b09 = None;
            let mut b10 = None;
            let mut b11 = None;
            let mut b12 = None;
            let mut tci = None;

            for node in data {
                let obj: DataObject<'a> = serde_roxmltree::from_node(node)?;

                if obj.id == "IMG_DATA_Band_60m_1_Tile1_Data" {
                    b01 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_10m_1_Tile1_Data" {
                    b02 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_10m_2_Tile1_Data" {
                    b03 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_10m_3_Tile1_Data" {
                    b04 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_20m_1_Tile1_Data" {
                    b05 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_20m_2_Tile1_Data" {
                    b06 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_20m_3_Tile1_Data" {
                    b07 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_10m_4_Tile1_Data" {
                    b08 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_20m_4_Tile1_Data" {
                    b8a = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_60m_2_Tile1_Data" {
                    b09 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_60m_3_Tile1_Data" {
                    b10 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_20m_5_Tile1_Data" {
                    b11 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_20m_6_Tile1_Data" {
                    b12 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_TCI_Tile1_Data" {
                    tci = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
            }

            let b01 = b01.context("could not find band `B01`")?;
            let b02 = b02.context("could not find band `B02`")?;
            let b03 = b03.context("could not find band `B03`")?;
            let b04 = b04.context("could not find band `B04`")?;
            let b05 = b05.context("could not find band `B05`")?;
            let b06 = b06.context("could not find band `B06`")?;
            let b07 = b07.context("could not find band `B07`")?;
            let b08 = b08.context("could not find band `B08`")?;
            let b8a = b8a.context("could not find band `B8a`")?;
            let b09 = b09.context("could not find band `B09`")?;
            let b10 = b10.context("could not find band `B10`")?;
            let b11 = b11.context("could not find band `B11`")?;
            let b12 = b12.context("could not find band `B12`")?;
            let tci = tci.context("could not find band `TCI`")?;

            Ok(L1CPaths {
                b01,
                b02,
                b03,
                b04,
                b05,
                b06,
                b07,
                b08,
                b8a,
                b09,
                b10,
                b11,
                b12,
                tci,
            })
        };

        let parse_l2a = || -> Result<L2APaths> {
            let data = self
                .doc
                .descendants()
                .filter(|n| n.has_tag_name("dataObject"));

            let mut b01 = None;
            let mut b02 = None;
            let mut b03 = None;
            let mut b04 = None;
            let mut b05 = None;
            let mut b06 = None;
            let mut b07 = None;
            let mut b08 = None;
            let mut b8a = None;
            let mut b09 = None;
            let mut b11 = None;
            let mut b12 = None;
            let mut tci = None;
            let mut aot = None;
            let mut wvp = None;
            let mut scl = None;

            for node in data {
                let obj: DataObject<'a> = serde_roxmltree::from_node(node)?;

                if obj.id == "IMG_DATA_Band_B01_20m_Tile1_Data" {
                    b01 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_B02_10m_Tile1_Data" {
                    b02 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_B03_10m_Tile1_Data" {
                    b03 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_B04_10m_Tile1_Data" {
                    b04 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_B05_20m_Tile1_Data" {
                    b05 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_B06_20m_Tile1_Data" {
                    b06 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_B07_20m_Tile1_Data" {
                    b07 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_B08_10m_Tile1_Data" {
                    b08 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_B8A_20m_Tile1_Data" {
                    b8a = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_B09_60m_Tile1_Data" {
                    b09 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_B11_20m_Tile1_Data" {
                    b11 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_B12_20m_Tile1_Data" {
                    b12 = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_TCI_10m_Tile1_Data" {
                    tci = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_AOT_10m_Tile1_Data" {
                    aot = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_WVP_10m_Tile1_Data" {
                    wvp = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
                if obj.id == "IMG_DATA_Band_SCL_20m_Tile1_Data" {
                    scl = Some(obj.byte_stream.file_location.href_without_dotslash());
                }
            }

            let b01 = b01.context("could not find band `B01`")?;
            let b02 = b02.context("could not find band `B02`")?;
            let b03 = b03.context("could not find band `B03`")?;
            let b04 = b04.context("could not find band `B04`")?;
            let b05 = b05.context("could not find band `B05`")?;
            let b06 = b06.context("could not find band `B06`")?;
            let b07 = b07.context("could not find band `B07`")?;
            let b08 = b08.context("could not find band `B08`")?;
            let b8a = b8a.context("could not find band `B8a`")?;
            let b09 = b09.context("could not find band `B09`")?;
            let b11 = b11.context("could not find band `B11`")?;
            let b12 = b12.context("could not find band `B12`")?;
            let tci = tci.context("could not find band `TCI`")?;
            let aot = aot.context("could not find band `AOT`")?;
            let wvp = wvp.context("could not find band `WVP`")?;
            let scl = scl.context("could not find band `SCL`")?;

            Ok(L2APaths {
                aot,
                b01,
                b02,
                b03,
                b04,
                b05,
                b06,
                b07,
                b08,
                b8a,
                b09,
                b11,
                b12,
                tci,
                wvp,
                scl,
            })
        };

        let paths = match unit_type {
            "Product_Level-1C" => Paths::L1C(parse_l1c()?),
            "Product_Level-2A" => Paths::L2A(parse_l2a()?),
            e => {
                anyhow::bail!("unknown unitType '{e}'")
            }
        };

        Ok(paths)
    }
}

#[cfg(test)]
mod tests {
    use crate::manifest::{ManifestPathsExtractor, Paths};

    const SAMPLE_L1C: &str = include_str!(
        "../test-data/S2B_MSIL1C_20211107T210529_N0500_R071_T01CCV_20221229T071512-manifest.safe"
    );
    const SAMPLE_L2A: &str = include_str!(
        "../test-data/S2B_MSIL2A_20250120T210529_N0511_R071_T01CCV_20250121T000408-manifest.safe"
    );

    #[test]
    fn test_full_file_l1c() {
        let extractor = ManifestPathsExtractor::try_new(SAMPLE_L1C).unwrap();
        let paths = extractor.extract().unwrap();
        matches!(paths, Paths::L1C(_));
    }

    #[test]
    fn test_full_file_l2a() {
        let extractor = ManifestPathsExtractor::try_new(SAMPLE_L2A).unwrap();
        let paths = extractor.extract().unwrap();
        matches!(paths, Paths::L2A(_));
    }
}
