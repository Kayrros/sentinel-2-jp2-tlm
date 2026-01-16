#[macro_use]
extern crate parquet_derive;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use clap::Parser;
use opendal::{
    Operator,
    layers::{LoggingLayer, RetryLayer},
    services::S3,
};
use parquet::ParquetWriter;
use tokio::{fs::File, io::AsyncReadExt};

use indexer::manifest;

mod parquet;
mod rate_limit;

#[derive(Parser)]
struct Cli {
    product_ids_path: PathBuf,
    parallel: usize,
    kv_store_path: PathBuf,

    #[clap(long, default_value = "false")]
    read_manifests: bool,

    /// S3 bucket, it should not be present in the s3 paths
    #[clap(long, env = "AWS_BUCKET")]
    bucket: String,

    /// S3 endpoint
    #[arg(long, env = "AWS_ENDPOINT")]
    endpoint: String,

    /// S3 access key id
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    access_key_id: String,

    /// S3 secret access key
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    secret_access_key: String,

    /// Max requests per second
    #[clap(long, default_value = "0")]
    rate_limit: u32,
}

#[derive(Debug, Clone, ParquetRecordWriter)]
struct Entry {
    product_id: String,
    band_id: String,
    path: String,
    index: Vec<u8>,
}

#[derive(Debug, Clone)]
struct SafeRoot {
    product_id: String,
    path: String,
}

impl SafeRoot {
    fn manifest_path(&self) -> String {
        format!("{}/manifest.safe", self.path)
    }
}

#[derive(Debug, Clone)]
struct JP2Path {
    product_id: String,
    band_id: String,
    path: String,
}

async fn list_roots(path: &Path) -> Result<Vec<SafeRoot>> {
    let mut file = File::open(path).await?;

    let mut content = String::new();
    file.read_to_string(&mut content).await?;

    let mut roots = vec![];
    for line in content.lines() {
        if !line.is_empty() {
            let parts: Vec<&str> = line.split(',').collect();
            let root = SafeRoot {
                product_id: parts[0].to_string(),
                path: parts[1].trim_end_matches('/').to_string(),
            };

            anyhow::ensure!(root.path.starts_with('/'));
            anyhow::ensure!(!root.path.ends_with("/"));

            roots.push(root);
        }
    }

    Ok(roots)
}

async fn list_image_paths(path: &Path) -> Result<Vec<JP2Path>> {
    let mut file = File::open(path).await?;

    let mut content = String::new();
    file.read_to_string(&mut content).await?;

    let mut paths = vec![];
    for line in content.lines() {
        if !line.is_empty() {
            let parts: Vec<&str> = line.split(',').collect();
            let path = JP2Path {
                product_id: parts[0].to_string(),
                band_id: parts[1].to_string(),
                path: parts[2].to_string(),
            };

            anyhow::ensure!(path.path.starts_with('/'));
            anyhow::ensure!(path.path.ends_with(".jp2"));

            paths.push(path);
        }
    }

    Ok(paths)
}

async fn read_manifest(operator: &Operator, root: &SafeRoot) -> Result<Vec<JP2Path>> {
    let manifest_path = root.manifest_path();
    let content = operator.read(&manifest_path).await?.to_vec();
    let content = String::from_utf8(content)?;
    let extractor = manifest::ManifestPathsExtractor::try_new(&content)?;
    let paths = extractor.extract()?;

    let manifestpath_to_jp2path = |band_id: &str, band_path: &str| -> JP2Path {
        JP2Path {
            product_id: root.product_id.clone(),
            band_id: band_id.to_string(),
            path: format!("{}/{}", root.path, band_path),
        }
    };

    let jp2paths = match paths {
        manifest::Paths::L1C(paths) => {
            vec![
                manifestpath_to_jp2path("B01", paths.b01),
                manifestpath_to_jp2path("B02", paths.b02),
                manifestpath_to_jp2path("B03", paths.b03),
                manifestpath_to_jp2path("B04", paths.b04),
                manifestpath_to_jp2path("B05", paths.b05),
                manifestpath_to_jp2path("B06", paths.b06),
                manifestpath_to_jp2path("B07", paths.b07),
                manifestpath_to_jp2path("B08", paths.b08),
                manifestpath_to_jp2path("B8A", paths.b8a),
                manifestpath_to_jp2path("B09", paths.b09),
                manifestpath_to_jp2path("B10", paths.b10),
                manifestpath_to_jp2path("B11", paths.b11),
                manifestpath_to_jp2path("B12", paths.b12),
            ]
        }
        manifest::Paths::L2A(paths) => {
            vec![
                manifestpath_to_jp2path("B01", paths.b01),
                manifestpath_to_jp2path("B02", paths.b02),
                manifestpath_to_jp2path("B03", paths.b03),
                manifestpath_to_jp2path("B04", paths.b04),
                manifestpath_to_jp2path("B05", paths.b05),
                manifestpath_to_jp2path("B06", paths.b06),
                manifestpath_to_jp2path("B07", paths.b07),
                manifestpath_to_jp2path("B08", paths.b08),
                manifestpath_to_jp2path("B8A", paths.b8a),
                manifestpath_to_jp2path("B09", paths.b09),
                manifestpath_to_jp2path("B11", paths.b11),
                manifestpath_to_jp2path("B12", paths.b12),
                manifestpath_to_jp2path("SCL", paths.scl),
            ]
        }
    };

    Ok(jp2paths)
}

async fn read_manifests(
    operator: Arc<Operator>,
    roots: Vec<SafeRoot>,
    parallel: usize,
) -> Result<Vec<JP2Path>> {
    let (send, recv) = async_channel::unbounded();
    let (send_result, mut recv_result) = tokio::sync::mpsc::unbounded_channel();

    async fn reader(
        operator: Arc<Operator>,
        recv: async_channel::Receiver<SafeRoot>,
        send_result: tokio::sync::mpsc::UnboundedSender<JP2Path>,
    ) -> Result<()> {
        while let Ok(root) = recv.recv().await {
            let paths = read_manifest(&operator, &root).await;
            match paths {
                Ok(paths) => {
                    for path in paths {
                        let _ = send_result.send(path);
                    }
                }
                Err(e) => log::error!("reading manifest for {}: {}", root.path, e),
            }
        }
        Ok(())
    }

    for _ in 0..parallel {
        tokio::spawn(reader(operator.clone(), recv.clone(), send_result.clone()));
    }
    drop(send_result);

    for root in roots {
        send.send(root).await?;
    }
    send.close();

    let mut jp2paths = vec![];
    while let Some(path) = recv_result.recv().await {
        jp2paths.push(path);
    }

    Ok(jp2paths)
}

async fn index_file(operator: Arc<Operator>, jp2: JP2Path) -> Result<Entry> {
    let path = &jp2.path;
    log::debug!("running on {path:?}");
    let stat = operator.stat(path).await?;
    let length = stat.content_length() as usize;
    log::debug!("length: {length}");
    let reader = operator.reader(path).await?;
    let idx = indexer::make_index(reader, length).await?;
    log::debug!("ok on {path:?}");
    Ok(Entry {
        product_id: jp2.product_id,
        band_id: jp2.band_id,
        path: jp2.path,
        index: idx,
    })
}

async fn run_indexing_in_background(
    operator: Arc<Operator>,
    paths: Vec<JP2Path>,
    parallel: usize,
    send_results: tokio::sync::mpsc::Sender<Result<Entry>>,
) {
    let (send, recv) = async_channel::unbounded();

    for _ in 0..parallel {
        let recv = recv.clone();
        let operator = operator.clone();
        let send_results = send_results.clone();
        tokio::spawn(async move {
            while let Ok(path) = recv.recv().await {
                let index = index_file(operator.clone(), path).await;
                let _ = send_results.send(index).await;
            }
        });
    }

    for path in paths {
        let _ = send.send(path).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init_timed();

    let cli = Cli::parse();

    let builder = S3::default()
        .bucket(&cli.bucket)
        .root("/")
        .disable_ec2_metadata()
        .endpoint(&cli.endpoint)
        .access_key_id(&cli.access_key_id)
        .secret_access_key(&cli.secret_access_key);
    let operator = Operator::new(builder)
        .unwrap()
        .layer(LoggingLayer::default())
        .layer(RetryLayer::default());
    let operator = if cli.rate_limit > 0 {
        operator
            .layer(rate_limit::ThrottleLayer::new(cli.rate_limit * 60))
            .finish()
    } else {
        operator.finish()
    };
    let operator = Arc::new(operator);

    let paths = {
        let mut paths = if cli.read_manifests {
            let roots = list_roots(&cli.product_ids_path).await?;
            read_manifests(operator.clone(), roots, cli.parallel).await?
        } else {
            list_image_paths(&cli.product_ids_path).await?
        };

        // idealy the sort would make the parquet sorted, but because of parallelism we lose the
        // sorting later
        paths.sort_by(|p1, p2| p1.product_id.cmp(&p2.product_id));
        paths
    };

    let todo = paths.len();
    log::info!("number of paths to process: {todo}");

    let mut writer = ParquetWriter::new(&cli.kv_store_path, 500)?;

    let (sender, mut recv) = tokio::sync::mpsc::channel(cli.parallel);
    run_indexing_in_background(operator, paths, cli.parallel, sender).await;

    let mut done = 0;

    while let Some(r) = recv.recv().await {
        match r {
            Ok(r) => {
                log::debug!("len: {}", r.index.len());
                writer.append(r)?;
            }
            Err(e) => log::error!("indexing: {e}"),
        }

        done += 1;
        if done % cli.parallel == 0 {
            log::info!("processed: {done}/{todo}");
        }
    }

    writer.close()?;

    Ok(())
}
