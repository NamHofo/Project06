import pandas as pd
import glob
import numpy as np

# Đường dẫn đến các file Parquet
parquet_files = glob.glob("gs://project-06-bucket/exports_parquet/export_batch_*.parquet")

for file in parquet_files:
    try:
        # Đọc file Parquet
        df = pd.read_parquet(file)

        # Chuẩn hóa cột key_search thành STRING
        if 'key_search' in df.columns:
            def to_string(value):
                if pd.isna(value):
                    return None
                if isinstance(value, (int, float)):
                    return str(value)
                if isinstance(value, bool):
                    return str(value).lower()
                if isinstance(value, str):
                    return value
                return None

            df['key_search'] = df['key_search'].apply(to_string)

        # Chuẩn hóa cột is_paypal thành FLOAT
        if 'is_paypal' in df.columns:
            def to_float(value):
                if pd.isna(value):
                    return None
                if isinstance(value, bool):
                    return 1.0 if value else 0.0
                if isinstance(value, (int, float)):
                    return 1.0 if value == 1 else 0.0 if value == 0 else None
                if isinstance(value, str):
                    value = value.lower().strip()
                    if value in ['true', '1', 'yes']:
                        return 1.0
                    if value in ['false', '0', 'no']:
                        return 0.0
                    return None
                return None

            df['is_paypal'] = df['is_paypal'].apply(to_float)

        # Chuẩn hóa cột cat_id thành STRING
        if 'cat_id' in df.columns:
            df['cat_id'] = df['cat_id'].apply(to_string)

        # Chuẩn hóa cột local_time thành TIMESTAMP
        if 'local_time' in df.columns:
            def to_timestamp(value):
                if pd.isna(value):
                    return None
                try:
                    # Chuyển chuỗi thành timestamp
                    return pd.to_datetime(value)
                except (ValueError, TypeError):
                    return None

            df['local_time'] = df['local_time'].apply(to_timestamp)

        # Lưu lại file Parquet
        df.to_parquet(file, index=False)
        print(f"Updated {file}")

    except Exception as e:
        print(f"Error processing {file}: {str(e)}")