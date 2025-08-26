from pydantic_settings import BaseSettings
from typing import Any, Optional
from fastapi_cognito import CognitoAuth, CognitoSettings, CognitoToken


class Settings(BaseSettings):
    check_expiration: bool = True
    jwt_header_prefix: str = "Bearer"
    jwt_header_name: str = "Authorization"
    aws_region: str
    cognito_userpool_id: str
    
    userpools: dict[str, dict[str, Any]] = {}
    
    def __init__(self, **data):
        super().__init__(**data)
        if not self.userpools:
            self.userpools = {
                "us": {
                    "region": self.aws_region,
                    "userpool_id": self.cognito_userpool_id,
                    "app_client_id": ""
                }
            }
    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"


cognito_settings = Settings()


class CognitoTokenSemUsername(CognitoToken):
    username: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        if self.username is None:
            self.username = self.client_id


cognito = CognitoAuth(
    settings=CognitoSettings.from_global_settings(cognito_settings),
    userpool_name="us",
    custom_model=CognitoTokenSemUsername,
)
