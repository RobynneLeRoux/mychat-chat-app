import json
from asyncio import StreamReader, StreamWriter
from datetime import datetime
from typing import Any, Self

from helpers import Chat


class Packet:
    category: str  # CMD, CTL, DATA
    msg_type: str  # LOGIN, ACK, SEND
    headers: dict[str, Any] = {}
    body: str
    LINE_SEPARATOR = "\r\n"
    BODY_SEPARATOR = "\r\n\r\n"

    def __init__(self, category: str, msg_type: str, headers: dict[str, Any], body=""):
        self.category = category
        self.msg_type = msg_type
        self.headers = headers
        self.body = body

    def encode(self) -> bytes:
        """Called on an instance of Packet, returns a string formatted according to our protocol spec that can be sent to the server."""

        lines = [f"{self.category} {self.msg_type}"]
        for key, val in self.headers.items():
            lines.append(f"{key} {val}")

        if self.body:
            lines.append(f"ContentLength {len(self.body.encode())}")

        raw = Packet.LINE_SEPARATOR.join(lines) + Packet.BODY_SEPARATOR
        if self.body:
            raw += self.body

        return raw.encode()

    @staticmethod
    async def read_message(reader: StreamReader) -> "Packet":
        # Read until the header/body separator
        header_bytes = await reader.readuntil(Packet.BODY_SEPARATOR.encode())
        header_part = header_bytes.decode().removesuffix(Packet.BODY_SEPARATOR)

        # Parse ContentLength from headers if present
        content_length = 0
        # skip the first line (category msg_type)
        for line in header_part.split(Packet.LINE_SEPARATOR)[1:]:
            if line.upper().startswith("CONTENTLENGTH"):
                content_length = int(line.split(" ", 1)[1])
                break

        # Read exactly content_length bytes for the body
        body = ""
        if content_length > 0:
            # readexactly is an asyncio StreamReader method that
            # reads exactly the number of bytes specified, or
            # raises an exception if the stream ends before that
            # many bytes can be read
            body_bytes = await reader.readexactly(content_length)
            body = body_bytes.decode()

        return Packet.decode((header_part + Packet.BODY_SEPARATOR + body).encode())

    @staticmethod
    def decode(raw_bytes: bytes) -> "Packet":
        """
        Static - called on the class itself, not an instance.
        Used for parsing raw bytes into a Packet object.
        """

        header_part, _, body = raw_bytes.decode().partition(Packet.BODY_SEPARATOR)
        lines = header_part.split(Packet.LINE_SEPARATOR)
        category, msg_type = lines[0].split()
        headers = {}
        for line in lines[1:]:
            if line and not line.upper().startswith("CONTENTLENGTH"):
                key, _, val = line.partition(" ")

                if val.lower() in ["true", "false"]:
                    val = val.lower() == "true"

                headers[key] = val

        return Packet(category, msg_type, headers, body)

    def __str__(self):  # equivalent to toString() in Java
        lines = [f"{self.category} {self.msg_type}"]
        for key, val in self.headers.items():
            lines.append(f"  {key}: {val}")
        if self.body:
            lines.append(f"  [body]: {self.body}")
        return "\n".join(lines)

    async def send_to_writer(self, writer: StreamWriter):
        writer.write(self.encode())
        await writer.drain()

    @property
    def is_empty(self) -> bool:
        return len(self.headers) == 0

    """Futures"""

    def is_future(self) -> bool:
        return "futureId" in self.headers

    def get_future_id(self) -> str:
        return self.headers["futureId"]

    @property
    def is_ctl(self) -> bool:
        return self.category == "CTL"

    @property
    def is_cmd(self) -> bool:
        return self.category == "CMD"

    @property
    def is_inf(self) -> bool:
        return self.category == "INF"

    """INF LOGIN"""
    # INFO: NEW PROTOCOL ITEM

    @staticmethod
    async def send_inf_login_packet(writer, user_id, username):
        await Packet(
            "INF", "LOGIN", {"userId": user_id, "username": username}
        ).send_to_writer(writer)

    def is_inf_login(self) -> bool:
        return self.is_inf and self.msg_type == "LOGIN"

    def get_user_from_inf_login(self) -> tuple[str, str]:
        return self.headers["userId"], self.headers["username"]

    """INF LOGOUT"""

    # INFO: NEW PROTOCOL ITEM
    @staticmethod
    async def send_inf_logout_packet(writer, user_id, username):
        await Packet(
            "INF", "LOGOUT", {"userId": user_id, "username": username}
        ).send_to_writer(writer)

    def is_inf_logout(self) -> bool:
        return self.is_inf and self.msg_type == "LOGOUT"

    def get_user_from_inf_logout(self) -> tuple[str, str]:
        return self.headers["userId"], self.headers["username"]

    """DATA SEND"""

    @staticmethod
    async def send_data_send_packet(writer: StreamWriter, chat_id: str, content: str):
        # INFO: Client requests that a message is sent
        await Packet("DATA", "SEND", {"chatId": chat_id}, content).send_to_writer(
            writer
        )

    def is_data_send(self) -> bool:
        return self.category == "DATA" and self.msg_type == "SEND"

    def get_message_from_data_send(self) -> str:
        return self.body

    def get_chat_id_from_data_send(self) -> str:
        return self.headers["chatId"]

    def get_sent_at(self) -> datetime:
        return datetime.now()

    """INF SEND"""

    @staticmethod
    async def send_inf_send_packet(
        writer: StreamWriter,
        sender_id: str,
        sender_username: str,
        chat_id: str,
        content: str,
    ):
        # INFO: SERVER responds with that direct message
        # ISSUE: Shouldn't need to send the from_name in this packet, client will know the username because it will keep track of iD?
        await Packet(
            "INF",
            "SEND",
            {
                "fromId": sender_id,
                "fromUsername": sender_username,
                "chatId": chat_id,
            },
            content,
        ).send_to_writer(writer)

    def is_inf_data(self) -> bool:
        return self.is_inf and self.msg_type == "SEND"

    def get_sender_user_from_inf_send(self) -> tuple[str, str]:
        # TODO: This prob only needs to return the user_id
        # The clients should be keeping track of usernames locally
        return self.headers["fromId"], self.headers["fromUsername"]

    """CMD SEEONLINE"""

    @staticmethod
    async def send_cmd_seeonline_packet(writer: StreamWriter):
        await Packet("CMD", "SEEONLINE", {}).send_to_writer(writer)

    def is_cmd_seeonline(self) -> bool:
        return self.is_cmd and self.msg_type == "SEEONLINE"

    """CTL SEEONLINE"""

    @staticmethod
    async def send_ctl_seeonline_packet(
        writer: StreamWriter, members_list: list[tuple[str, str]]
    ):
        await Packet("CTL", "SEEONLINE", {}, json.dumps(members_list)).send_to_writer(
            writer
        )

    def is_ctl_seeonline(self) -> bool:
        return self.is_ctl and self.msg_type == "SEEONLINE"

    def get_members_list_from_ctl_seeonline(self) -> list[tuple[str, str]]:
        return json.loads(self.body)

    """CMD SEEGROUPS"""

    @staticmethod
    async def send_cmd_seegroups_packet(writer: StreamWriter):
        await Packet("CMD", "SEEGROUPS", {}).send_to_writer(writer)

    def is_cmd_seegroups(self) -> bool:
        return self.is_cmd and self.msg_type == "SEEGROUPS"

    """CTL SEEGROUPS"""

    @staticmethod
    async def send_ctl_seegroups_packet(
        writer: StreamWriter, groups_list: list[tuple[str, str]]
    ):
        await Packet("CTL", "SEEGROUPS", {}, json.dumps(groups_list)).send_to_writer(
            writer
        )

    def is_ctl_seegroups(self) -> bool:
        return self.is_ctl and self.msg_type == "SEEGROUPS"

    def get_groups_list_from_ctl_seegroups(self) -> list[tuple[str, str]]:
        return json.loads(self.body)

    """CMD REGISTER"""

    @staticmethod
    async def send_cmd_register_packet(
        writer: StreamWriter, username: str, password: str
    ):
        await Packet(
            "CMD", "REGISTER", {"username": username, "password": password}
        ).send_to_writer(writer)

    def is_cmd_register(self) -> bool:
        return self.is_cmd and self.msg_type == "REGISTER"

    def get_username_from_cmd_register(self):
        return self.headers["username"]

    def get_password_from_cmd_register(self):
        return self.headers["password"]

    """CTL REGISTER"""

    @staticmethod
    async def send_ctl_register_packet(
        writer: StreamWriter, success: bool, error_msg: str = ""
    ):
        await Packet("CTL", "REGISTER", {"success": success}, error_msg).send_to_writer(
            writer
        )

    def is_ctl_register(self) -> bool:
        return self.is_ctl and self.msg_type == "REGISTER"

    def get_success_from_ctl_register(self) -> bool:
        assert type(self.headers["success"]) is bool
        return self.headers["success"]

    def get_error_message_from_ctl_registration(self) -> str:
        return self.body

    """CMD LOGIN"""

    @staticmethod
    async def send_cmd_login_packet(writer: StreamWriter, username: str, password: str):
        await Packet(
            "CMD", "LOGIN", {"username": username, "password": password}
        ).send_to_writer(writer)

    def is_cmd_login(self) -> bool:
        return self.is_cmd and self.msg_type == "LOGIN"

    def get_username_from_cmd_login(self) -> str:
        return self.headers["username"]

    def get_password_from_cmd_login(self) -> str:
        return self.headers["password"]

    """CTL LOGIN"""

    @staticmethod
    async def send_ctl_login_packet(
        writer: StreamWriter,
        success: bool,
        user_id: str = "",
        members_list: list[tuple[str, str]] = [],
        groups_list: list[tuple[str, str]] = [],
        error_msg: str = "",
    ):
        body = error_msg
        if success:
            body = json.dumps([user_id, members_list, groups_list])

        await Packet(
            "CTL",
            "LOGIN",
            {
                "success": success,
            },
            body,
        ).send_to_writer(writer)

    def is_ctl_login(self) -> bool:
        return self.is_ctl and self.msg_type == "LOGIN"

    def get_success_from_ctl_login(self) -> bool:
        assert type(self.headers["success"]) is bool
        return self.headers["success"]

    def get_error_message_from_ctl_login(self) -> str:
        return self.body

    def get_user_id_from_ctl_login(self) -> str:
        assert self.headers["success"]
        return json.loads(self.body)[0]

    def get_members_list_from_ctl_login(self) -> list[tuple[str, str]]:
        assert self.headers["success"]
        return json.loads(self.body)[1]

    def get_groups_list_from_ctl_login(self) -> list[tuple[str, str]]:
        assert self.headers["success"]
        return json.loads(self.body)[2]

    """CMD CREATE (Group chat)"""

    @staticmethod
    async def send_cmd_create_packet(
        writer: StreamWriter, group_name: str, future_id: str
    ):
        await Packet(
            "CMD", "CREATE", {"groupName": group_name, "futureId": future_id}
        ).send_to_writer(writer)

    def is_cmd_create(self) -> bool:
        return self.is_cmd and self.msg_type == "CREATE"

    def get_group_name_from_cmd_create(self) -> str:
        return self.headers["groupName"]

    """CTL CREATE (Group chat)"""

    @staticmethod
    async def send_ctl_create_packet(
        writer: StreamWriter, future_id: str, success: bool, error_msg: str = ""
    ):
        await Packet(
            "CTL",
            "CREATE",
            {
                "futureId": future_id,
                "success": success,
            },
            error_msg,
        ).send_to_writer(writer)

    @staticmethod
    def make_failed_ctl_create_packet(error_msg: str) -> Self:
        return Packet("CTL", "CREATE", {"success": False}, error_msg)

    def is_ctl_create_success(self) -> bool:
        return self.is_ctl and self.msg_type == "CREATE" and self.headers["success"]

    def get_error_msg_from_ctl_create(self) -> str:
        return self.body

    """CMD JOIN (Group chat)"""

    @staticmethod
    async def send_cmd_join_packet(writer: StreamWriter, chat_id: str, future_id: str):
        await Packet(
            "CMD", "JOIN", {"chatId": chat_id, "futureId": future_id}
        ).send_to_writer(writer)

    def is_cmd_join(self) -> bool:
        return self.is_cmd and self.msg_type == "JOIN"

    def get_chat_id_from_cmd_join(self) -> str:
        return self.headers["chatId"]

    """CTL JOIN (Group Chat)"""

    @staticmethod
    async def send_ctl_join_packet(
        writer: StreamWriter,
        future_id: str,
        success: bool,
        user_ids: list[str] = [],
        error_msg: str = "",
    ):
        body = error_msg
        if success:
            body = json.dumps(user_ids)
        await Packet(
            "CTL", "JOIN", {"futureId": future_id, "success": success}, body
        ).send_to_writer(writer)

    @staticmethod
    def make_failed_ctl_join_packet(error_msg: str) -> Self:
        return Packet("CTL", "JOIN", {"success": False}, error_msg)

    def is_ctl_join(self) -> bool:
        return self.is_ctl and self.msg_type == "JOIN"

    def get_user_ids_from_ctl_join(self) -> list[str]:
        assert self.headers["success"]
        return json.loads(self.body)

    def get_success_from_ctl_join(self) -> bool:
        return self.headers["success"]

    def get_error_message_from_ctl_join(self) -> str:
        return self.body

    """INF JOIN"""

    # INFO: NEW PROTOCOL ITEM
    @staticmethod
    async def send_inf_join_packet(
        writer: StreamWriter, chat_id: str, user_id: str, username: str
    ):
        await Packet(
            "INF",
            "JOIN",
            {
                "chatId": chat_id,
                "userId": user_id,
                "username": username,
            },
        ).send_to_writer(writer)

    def is_inf_join(self) -> bool:
        return self.is_inf and self.msg_type == "JOIN"

    def get_chat_id_from_inf_join(self) -> str:
        return self.headers["chatId"]

    def get_username_from_inf_join(self) -> str:
        return self.headers["username"]

    def get_user_id_from_inf_join(self) -> str:
        return self.headers["userId"]

    """INF CREATE"""

    @staticmethod
    async def send_inf_create_packet(
        writer: StreamWriter, chat_id: str, group_name: str
    ):
        await Packet(
            "INF", "CREATE", {"chatId": chat_id, "groupName": group_name}
        ).send_to_writer(writer)

    def is_inf_create(self) -> bool:
        return self.is_inf and self.msg_type == "CREATE"

    def get_group_from_inf_create(self) -> Chat:
        return Chat(self.headers["chatId"], {}, False, self.headers["groupName"])

    """CMD LEAVE (Group chat)"""

    @staticmethod
    async def send_cmd_leave_packet(writer: StreamWriter, chat_id: str):
        await Packet("CMD", "LEAVE", {"chatId": chat_id}).send_to_writer(writer)

    def is_cmd_leave(self) -> bool:
        return self.is_cmd and self.msg_type == "LEAVE"

    def get_chat_id_from_cmd_leave(self) -> str:
        return self.headers["chatId"]

    """INF LEAVE (Group chat)"""

    @staticmethod
    async def send_inf_leave_packet(
        writer: StreamWriter, chat_id: str, user_id: str, username: str
    ):
        await Packet(
            "INF", "LEAVE", {"chatId": chat_id, "userId": user_id, "username": username}
        ).send_to_writer(writer)

    def is_inf_leave(self) -> bool:
        return self.is_inf and self.msg_type == "LEAVE"

    def get_chat_id_from_inf_leave(self) -> str:
        return self.headers["chatId"]

    def get_user_id_from_inf_leave(self) -> str:
        return self.headers["userId"]

    def get_username_from_inf_leave(self) -> str:
        return self.headers["username"]

    """CTL ERROR"""

    @staticmethod
    async def send_ctl_error_packet(writer: StreamWriter, error_msg: str):
        await Packet("CTL", "ERROR", {"error_msg": error_msg}).send_to_writer(writer)

    def is_ctl_error(self) -> bool:
        return self.is_ctl and self.msg_type == "ERROR"

    def get_error_message_from_ctl_error(self) -> str:
        return self.headers.get("error_msg", "Unknown error.")

    """File Transfer"""

    @staticmethod
    async def send_file_request_packet(
        writer: StreamWriter,
        chat_id: str,
        transfer_id: str,
        filepath: str,
        filename: str,
        filesize: int,
        port: str,
    ):
        await Packet(
            "DATA",
            "MEDIA",
            {
                "chatId": chat_id,
                "transferId": transfer_id,
                "filepath": filepath,
                "filename": filename,
                "filesize": filesize,
                "port": port,
            },
        ).send_to_writer(writer)

    def is_file_request(self) -> bool:
        return self.category == "DATA" and self.msg_type == "MEDIA"

    @staticmethod
    async def send_file_response_packet(
        writer: StreamWriter,
        sender_id: str,
        sender_username: str,
        chat_id: str,
        transfer_id: str,
        filepath: str,
        filename: str,
        filesize: int,
        port: str,
    ):
        await Packet(
            "INF",
            "MEDIA",
            {
                "fromId": sender_id,
                "username": sender_username,
                "chatId": chat_id,
                "transferId": transfer_id,
                "filepath": filepath,
                "filename": filename,
                "filesize": filesize,
                "port": port,
            },
        ).send_to_writer(writer)

    def is_file_response(self) -> bool:
        return self.is_inf and self.msg_type == "MEDIA"

    def get_sender_user_from_file_send(self) -> tuple[str, str]:
        return self.headers["fromId"], self.headers["username"]

    def get_filename_from_file_send(self) -> str:
        return self.headers["filename"]

    def get_filepath_from_file_send(self) -> str:
        return self.headers["filepath"]

    def get_filesize_from_file_send(self) -> int:
        return int(self.headers["filesize"])

    def get_transfer_id_from_file_send(self) -> str:
        return self.headers["transferId"]

    def get_chat_id_from_file_send(self) -> str:
        return self.headers["chatId"]

    def get_port(self) -> str:
        return self.headers["port"]
