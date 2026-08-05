from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    app_name: str = "UCI Dataset Replayer"
    file_path: str = "data/household_power_consumption.txt"
    default_house_id: str = "house_001"
    default_rate: float = 1.0

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
