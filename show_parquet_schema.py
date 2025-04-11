import pyarrow.parquet as pq

# Kiểm tra một vài file ví dụ
files = [
    "gs://project-06-bucket/exports_parquet/export_batch_99_20250411_013204.parquet",
    "gs://project-06-bucket/exports_parquet/export_batch_90_20250411_013201.parquet"
]

for file_path in files:
    table = pq.read_table(file_path)
    print(f"Schema của {file_path}:")
    print(table.schema)