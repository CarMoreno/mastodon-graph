from pydantic import BaseModel, computed_field, Field


class User(BaseModel):
    id: str
    username: str
    acct: str
    instance: str

    @computed_field
    @property
    def server(self) -> str:
        if "@" in self.acct:
            return self.acct.split("@")[1]
        return "mastodon.social"


class Author(User):
    status_id: str


class Reblogger(User):
    author_id: str


class RebloggedBy(BaseModel):
    author_id: str
    reblogged_count: int = Field(ge=0, default=1)
    reblogger_id: str
