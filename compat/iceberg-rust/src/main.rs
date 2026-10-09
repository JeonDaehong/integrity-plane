//! iceberg-rust against the integrity gateway: valid appends are certified; violations fail with
//! the integrity code after exactly one commit attempt.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};
use parquet::file::properties::WriterProperties;

const GATEWAY: &str = "http://127.0.0.1:8181";
const NS: &str = "irust";

type Res<T> = Result<T, Box<dyn std::error::Error>>;

async fn commits(table: &str) -> Res<u64> {
    let status: serde_json::Value = reqwest::get(format!("{GATEWAY}/v1/integrity/status")).await?.json().await?;
    Ok(status["commit_requests"][format!("{NS}.{table}")].as_u64().unwrap_or(0))
}

fn schema(second: &str, second_type: PrimitiveType) -> Res<Schema> {
    Ok(Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, second, Type::Primitive(second_type)).into(),
        ])
        .build()?)
}

static FILES: AtomicU32 = AtomicU32::new(0);

/// Writes one Parquet file with the given columns and fast-appends it.
async fn append(catalog: &dyn Catalog, table: &Table, columns: Vec<ArrayRef>) -> iceberg::Result<Table> {
    let arrow = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
    let batch = RecordBatch::try_new(arrow, columns).map_err(|e| iceberg::Error::new(iceberg::ErrorKind::Unexpected, e.to_string()))?;
    let parquet = ParquetWriterBuilder::new(WriterProperties::default(), table.metadata().current_schema().clone());
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet,
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata())?,
        DefaultFileNameGenerator::new(format!("f{}", FILES.fetch_add(1, Ordering::SeqCst)), None, DataFileFormat::Parquet),
    );
    let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
    writer.write(batch).await?;
    let files = writer.close().await?;
    let tx = Transaction::new(table);
    let tx = tx.fast_append().add_data_files(files).apply(tx)?;
    tx.commit(catalog).await
}

async fn expect_rejection(code: &str, name: &str, attempt: impl std::future::Future<Output = iceberg::Result<Table>>) -> Res<()> {
    let before = commits(name).await?;
    match attempt.await {
        Ok(_) => Err(format!("{code}: append was accepted").into()),
        Err(e) => {
            let message = e.to_string();
            if !message.contains(code) {
                return Err(format!("expected {code}, got: {message}").into());
            }
            let attempts = commits(name).await? - before;
            if attempts != 1 {
                return Err(format!("{code}: {attempts} commit attempts reached the gateway").into());
            }
            println!("ok   {code} rejected, {attempts} attempt");
            Ok(())
        }
    }
}

fn longs(v: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(v.to_vec()))
}

fn strings(v: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(v.to_vec()))
}

#[tokio::main]
async fn main() -> Res<()> {
    // The compatibility warehouse is a local filesystem path shared with the catalog.
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("gw", HashMap::from([(REST_CATALOG_PROP_URI.to_string(), GATEWAY.to_string())]))
        .await?;
    let ns = NamespaceIdent::new(NS.to_string());
    if !catalog.namespace_exists(&ns).await? {
        catalog.create_namespace(&ns, HashMap::new()).await?;
    }
    let create = |name: &str, schema: Schema| TableCreation::builder().name(name.to_string()).schema(schema).build();
    let customer = catalog.create_table(&ns, create("customer", schema("name", PrimitiveType::String)?)).await?;
    let orders = catalog.create_table(&ns, create("orders", schema("customer_id", PrimitiveType::Long)?)).await?;

    let customer = append(&catalog, &customer, vec![longs(&[Some(1), Some(2)]), strings(&[Some("alice"), Some("bob")])]).await?;
    let summary = customer.metadata().current_snapshot().ok_or("no snapshot")?.summary();
    let cert = summary.additional_properties.get("integrity.cert").ok_or("no certificate in summary")?;
    println!("ok   customer append certified {}", &cert[..16]);

    let orders = append(&catalog, &orders, vec![longs(&[Some(10)]), longs(&[Some(1)])]).await?;
    println!("ok   order for an existing customer");

    expect_rejection("INT-005", "orders", append(&catalog, &orders, vec![longs(&[Some(11)]), longs(&[Some(999)])])).await?;
    let customer = catalog.load_table(&TableIdent::new(ns.clone(), "customer".into())).await?;
    expect_rejection("INT-003", "customer", append(&catalog, &customer, vec![longs(&[Some(2)]), strings(&[Some("dup")])])).await?;
    expect_rejection("INT-007", "customer", append(&catalog, &customer, vec![longs(&[Some(3)]), strings(&[None])])).await?;
    Ok(())
}
