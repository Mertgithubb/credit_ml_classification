import logging
import pandas as pd
from pathlib import Path
from typing import Union
from src.utils.error_messages import ErrorMessages 

# Setup logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# DataLoader class to handle loading CSV data files
class DataLoader:
    SUPPORTED_FILE_EXTENSIONS = {".csv"}

    def __init__(self, data_path: Union[str, Path]):
        self.data_path = self.__validate_file_path(data_path)
        logger.info(f"Data path set to {self.data_path}")

    def __validate_file_path(self, data_path):
        if not isinstance(data_path, (str, Path)):
            raise TypeError(ErrorMessages.INVALID_FILE_PATH_TYPE.value.format(type=type(data_path)))

        path = Path(data_path)
        if not path.exists():
            raise FileNotFoundError(f"Data path {path} does not exist.")
        return path

    def __check_file_extension(self, file_path: Path):
        if file_path.suffix.lower() not in self.SUPPORTED_FILE_EXTENSIONS:
            raise ValueError(ErrorMessages.UNSUPPORTED_FILE_EXTENSION.value.format(
                ext=file_path.suffix
            ))

    def load_data(self, file_name):
        file_path = self.data_path / file_name

        try:
            self.__check_file_extension(file_path)

            if not file_path.exists():
                raise FileNotFoundError(ErrorMessages.FILE_NOT_FOUND.value.format(
                    file=file_name, path=self.data_path
                ))

            df = pd.read_csv(file_path)

            if df.empty:
                raise ValueError(ErrorMessages.EMPTY_DATA_FILE.value)

            logger.info(f"Loaded data from {file_path}")
            return df

        except pd.errors.ParserError:
            raise ValueError(ErrorMessages.PARSER_ERROR.value)
        except Exception as e:
            raise RuntimeError(ErrorMessages.UNEXPECTED_ERROR.value.format(error=e))

