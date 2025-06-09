import os
from mlProject import logger
from mlProject.entity.config_entity import DataValidationConfig
import pandas as pd


class DataValidation:
    def __init__(self, config: DataValidationConfig):
        self.config = config

    def validate_all_columns(self) -> bool:
        try:
            # Load data and schema
            data = pd.read_csv(self.config.unzip_data_dir)
            all_schema = self.config.all_schema  # dict: {col_name: dtype}
            schema_cols = set(all_schema.keys())
            data_cols = set(data.columns)

            mismatched_columns = []

            # 1. Check for missing and extra columns
            missing_cols = schema_cols - data_cols
            extra_cols = data_cols - schema_cols

            if missing_cols:
                for col in missing_cols:
                    mismatched_columns.append(f"Missing expected column: {col}")

            if extra_cols:
                for col in extra_cols:
                    mismatched_columns.append(f"Unexpected column: {col}")

            # 2. Check data types for matching columns
            for col in schema_cols & data_cols:
                actual_dtype = str(data[col].dtype)
                expected_dtype = all_schema[col]
                if actual_dtype != expected_dtype:
                    mismatched_columns.append(
                        f"Column '{col}' has dtype '{actual_dtype}', expected '{expected_dtype}'"
                    )

            # 3. Decide final validation status
            validation_status = len(mismatched_columns) == 0

            # 4. Write results to status file
            with open(self.config.STATUS_FILE, 'w') as f:
                f.write(f"Validation status: {validation_status}\n")
                if not validation_status:
                    f.write("Issues found:\n")
                    for issue in mismatched_columns:
                        f.write(f"- {issue}\n")

            return validation_status

        except Exception as e:
            raise e
