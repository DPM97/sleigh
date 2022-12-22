use crate::peer::PersistentState;

pub struct Persist;

impl Persist {
    pub async fn write(data: &PersistentState) -> anyhow::Result<()> {
        tokio::fs::write("./db/persistent", bincode::serialize(data)?).await?;
        Ok(())
    }

    pub async fn read() -> anyhow::Result<PersistentState> {
        Ok(bincode::deserialize(
            &tokio::fs::read("./db/persistent").await?,
        )?)
    }
}
