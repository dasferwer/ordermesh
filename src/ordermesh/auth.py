import secrets
from typing import Annotated

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from ordermesh.config import get_settings

bearer = HTTPBearer(auto_error=False)


def authenticate(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
) -> str:
    if credentials is not None:
        for client_id, key in get_settings().api_keys.items():
            value = key.get_secret_value()
            if value and secrets.compare_digest(credentials.credentials.encode(), value.encode()):
                return client_id
    raise HTTPException(
        status_code=401,
        detail="Нужен действующий ключ клиента",
        headers={"WWW-Authenticate": "Bearer"},
    )


ClientId = Annotated[str, Depends(authenticate)]
