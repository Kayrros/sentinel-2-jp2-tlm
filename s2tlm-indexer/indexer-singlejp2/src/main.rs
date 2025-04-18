use anyhow::Result;
use clap::Parser;
use opendal::Operator;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(clap::Parser)]
struct Cli {
    input: String,
    output: PathBuf,

    #[arg(short, long)]
    full_jp2: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init_timed();

    let cli = Cli::parse();

    let builder = opendal::services::Fs::default().root("/");
    let operator = Operator::new(builder)?.finish();
    let operator = Arc::new(operator);

    let path = &cli.input;
    let path = std::fs::canonicalize(path)?;
    let path = &path.into_os_string().into_string().unwrap();
    let stat = operator.stat(path).await.unwrap();
    let length = stat.content_length() as usize;
    let reader = operator.reader(path).await.unwrap();

    let idx = indexer::make_index(reader.clone(), length).await.unwrap();

    let mut file = std::fs::File::create(cli.output)?;

    if cli.full_jp2 {
        let file_length = u64::from_be_bytes(idx[0..8].try_into().unwrap());
        let position_first_sot = u64::from_be_bytes(idx[8..16].try_into().unwrap());
        let _full_tlm_segment_length = u32::from_be_bytes(idx[16..20].try_into().unwrap());

        let buf = reader.read(0..position_first_sot).await?;
        let buf = buf.to_vec();
        file.write_all(&buf)?;

        file.write_all(&idx[20..])?;

        let buf = reader.read(position_first_sot..file_length).await?;
        let buf = buf.to_vec();
        file.write_all(&buf)?;
    } else {
        file.write_all(&idx)?;
    }

    Ok(())
}
