from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service settings; all environment variables use the WATER_ prefix."""

    model_config = SettingsConfigDict(
        env_prefix="WATER_",
        env_file=None,
        extra="ignore",
    )

    database_path: str = Field(default="/data/water.db")
    alarm_threshold: float = Field(default=10.0)
    recovery_threshold: float = Field(default=5.0)
    # Maximum tolerable gap between adjacent samples of one device. A wider
    # gap tentatively ends an open alarm as "interrupted" at the last sample.
    max_sample_gap_seconds: int = Field(default=600, gt=0)

    @model_validator(mode="after")
    def validate_thresholds(self) -> "Settings":
        if self.alarm_threshold <= self.recovery_threshold:
            raise ValueError(
                "WATER_ALARM_THRESHOLD must be greater than "
                "WATER_RECOVERY_THRESHOLD for hysteresis to be unambiguous"
            )
        return self
