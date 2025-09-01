import pandas as pd
import io
from typing import Optional
from utils.logging_config import get_logger

logger = get_logger(__name__)

class XLSXProcessor:
    """Service for processing XLSX files and converting them to CSV format"""

    def __init__(self):
        self.sheet_name = "smartjects"

    def read_xlsx_content(self, file_content: bytes) -> Optional[str]:
        """
        Read XLSX file content and convert 'smartjects' sheet to CSV format

        Args:
            file_content: XLSX file content as bytes

        Returns:
            CSV string content or None if error
        """
        try:
            # Read the XLSX file from bytes
            xlsx_file = io.BytesIO(file_content)

            # Try to read the specific sheet
            try:
                df = pd.read_excel(xlsx_file, sheet_name=self.sheet_name)
                logger.info(f"Successfully loaded sheet '{self.sheet_name}' with {len(df)} rows")
            except ValueError as e:
                # Sheet doesn't exist, list available sheets
                available_sheets = pd.ExcelFile(xlsx_file).sheet_names
                logger.error(f"Sheet '{self.sheet_name}' not found. Available sheets: {available_sheets}")
                raise ValueError(f"Sheet '{self.sheet_name}' not found in XLSX file. Available sheets: {', '.join(available_sheets)}")

            # Check if dataframe is empty
            if df.empty:
                logger.warning(f"Sheet '{self.sheet_name}' is empty")
                return None

            # Log column names for debugging
            logger.info(f"Columns in sheet: {list(df.columns)}")

            # Clean column names (remove extra spaces, normalize)
            df.columns = df.columns.str.strip()

            # Convert DataFrame to CSV string
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, sep=',', encoding='utf-8')
            csv_content = csv_buffer.getvalue()

            logger.info(f"Successfully converted XLSX to CSV format ({len(csv_content)} characters)")
            return csv_content

        except Exception as e:
            logger.error(f"Error processing XLSX file: {e}")
            raise Exception(f"Failed to process XLSX file: {str(e)}")

    def validate_xlsx_structure(self, file_content: bytes) -> dict:
        """
        Validate XLSX file structure and return information about it

        Args:
            file_content: XLSX file content as bytes

        Returns:
            Dictionary with validation information
        """
        try:
            xlsx_file = io.BytesIO(file_content)
            excel_file = pd.ExcelFile(xlsx_file)

            validation_info = {
                'valid': True,
                'sheets': excel_file.sheet_names,
                'has_smartjects_sheet': self.sheet_name in excel_file.sheet_names,
                'error': None
            }

            if validation_info['has_smartjects_sheet']:
                # Read the smartjects sheet to get more info
                df = pd.read_excel(xlsx_file, sheet_name=self.sheet_name)
                validation_info['smartjects_rows'] = len(df)
                validation_info['smartjects_columns'] = list(df.columns)
            else:
                validation_info['valid'] = False
                validation_info['error'] = f"Required sheet '{self.sheet_name}' not found"

            return validation_info

        except Exception as e:
            return {
                'valid': False,
                'error': str(e),
                'sheets': [],
                'has_smartjects_sheet': False
            }
