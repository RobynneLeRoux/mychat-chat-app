from dataclasses import dataclass, field


def encode_direct_chat_id(*user_ids: str) -> str:
    return "_".join(sorted(list(user_ids)))


def decode_direct_chat_id(chat_id: str) -> list[str]:
    return chat_id.split("_")


@dataclass()
class User:
    id: str
    username: str = field(compare=False)


@dataclass()
class Chat:
    id: str
    users: dict[str, User]  # user_id, writer
    is_direct: bool
    group_name: str | None = field(default=None)

    def get_users_except(self, user: User | str) -> list[User]:
        if isinstance(user, str):
            return [u for u in self.users.values() if u.id != user]
        elif isinstance(user, User):
            return [u for u in self.users.values() if u.id != user.id]

    def get_active_user(self, auth_user: User) -> User:
        assert self.is_direct
        return self.get_users_except(auth_user)[0]

    def get_name(self, auth_user: User) -> str:
        if self.is_direct:
            return self.get_active_user(auth_user).username
        else:
            return str(self.group_name)
