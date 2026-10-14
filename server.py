import asyncio
import uuid
from argparse import ArgumentParser
from asyncio import IncompleteReadError, StreamReader, StreamWriter
from socket import AF_INET, SO_REUSEADDR, SOCK_DGRAM, SOCK_STREAM, SOL_SOCKET, socket

from helpers import Chat, User, decode_direct_chat_id
from packet import Packet


class Server:
    udp_sock: socket

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.clients: dict[str, StreamWriter] = {}
        self.chats: dict[str, Chat] = {}
        self.users: dict[str, User] = {}
        self.login_details = {}
        # INFO: Useful for testing
        # self.login_details = {"1": "1", "2": "2"}
        self.udp_clients = {}  # username: (ip, port)

    async def broadcast_inf_login(self, user_id, username):
        """Helper function for server to send INF LOGIN to all users"""
        for user in self.users.values():
            if user.id != user_id:
                await Packet.send_inf_login_packet(
                    self.clients[user.id], user_id, username
                )

    async def broadcast_inf_logout(self, user_id, username):
        """Helper function for server to send INF LOGOUT to all users"""
        for u in self.users.values():
            await Packet.send_inf_logout_packet(self.clients[u.id], user_id, username)

    async def broadcast_inf_create(self, group_chat_id: str):
        """Helper function for server to send INF CREATE to all users"""
        for user in self.users.values():
            await Packet.send_inf_create_packet(
                self.clients[user.id],
                group_chat_id,
                str(self.chats[group_chat_id].group_name),
            )

    async def broadcast_inf_join(self, chat_id: str, new_user_id: str):
        """Helper function for server to send INF JOIN to users in a chat"""
        for user_id in self.chats[chat_id].users:
            if user_id != new_user_id:
                await Packet.send_inf_join_packet(
                    self.clients[user_id],
                    chat_id,
                    new_user_id,
                    self.users[new_user_id].username,
                )

    async def broadcast_inf_leave(self, chat_id: str, old_user_id: str):
        """Helper function for server to send INF LEAVE to users in a chat"""
        for user_id in self.chats[chat_id].users:
            if user_id != old_user_id:
                await Packet.send_inf_leave_packet(
                    self.clients[user_id],
                    chat_id,
                    old_user_id,
                    self.users[old_user_id].username,
                )

    async def broadcast_to_chat_except(
        self, chat_id: str, content: str, sender_id: str
    ):
        """Helper function for server to send INF DATA to users in a chat"""
        for user in self.chats[chat_id].users.values():
            if user.id != sender_id:
                if self.clients[user.id]:
                    # Send to the recipient
                    await Packet.send_inf_send_packet(
                        self.clients[user.id],
                        sender_id,
                        self.users[sender_id].username,
                        chat_id,
                        content,
                    )
                else:
                    # INFO: Edge case, a client sends a message to an offline user
                    # The UI should prevent the user from sending messages to a user
                    # If they have received the user_offline message
                    await Packet.send_inf_logout_packet(
                        self.clients[sender_id], user.id, user.username
                    )

    async def broadcast_to_chat_file_except(
        self,
        chat_id: str,
        sender_id: str,
        transfer_id: str,
        filepath: str,
        filename: str,
        filesize: int,
        port: str,
    ):
        """Helper function for server to send INF MEDIA to users in a chat"""
        for user in self.chats[chat_id].users.values():
            if user.id != sender_id:
                if self.clients[user.id]:
                    # Send to the recipient
                    await Packet.send_file_response_packet(
                        self.clients[user.id],
                        sender_id,
                        self.users[sender_id].username,
                        chat_id,
                        transfer_id,
                        filepath,
                        filename,
                        filesize,
                        port,
                    )
                else:
                    # INFO: Edge case, a client sends a message to an offline user
                    # The UI should prevent the user from sending messages to a user
                    # If they have received the user_offline message
                    await Packet.send_inf_logout_packet(
                        self.clients[sender_id], user.id, user.username
                    )

    async def handle_client(self, reader: StreamReader, writer: StreamWriter):
        """Called when a client makes a connection with Server. Not yet logged in."""
        user_id = str(uuid.uuid4())[:8]  # Short 8-character unique ID
        user: User | None = None

        try:
            # ── AUTH PHASE ────────────────────────────────────────────────────
            # INFO: First packet must be CMD REGISTER or CMD LOGIN
            packet = await Packet.read_message(reader)
            print(packet)

            if not packet.is_cmd_register() and not packet.is_cmd_login():
                await Packet.send_ctl_error_packet(
                    writer, "You must REGISTER or LOGIN first."
                )
                return

            # REGISTER
            if packet.is_cmd_register():
                username = packet.get_username_from_cmd_register()
                password = packet.get_password_from_cmd_register()

                if not username:
                    await Packet.send_ctl_register_packet(
                        writer, False, "Username cannot be empty."
                    )
                elif not password:
                    await Packet.send_ctl_register_packet(
                        writer, False, "Password cannot be empty."
                    )
                elif username in self.login_details:
                    await Packet.send_ctl_register_packet(
                        writer, False, f"Username '{username}' is already taken."
                    )
                else:
                    self.login_details[username] = password
                    print(f"{username} registered.")
                    await Packet.send_ctl_register_packet(writer, True)

                return  # INFO: Registration always closes the connection — client must re-connect to login

            # LOGIN
            assert packet.is_cmd_login()
            username = packet.get_username_from_cmd_login()
            password = packet.get_password_from_cmd_login()

            if not username or not password:
                await Packet.send_ctl_login_packet(
                    writer, False, error_msg="Missing username or password."
                )
                return
            elif username not in self.login_details:
                await Packet.send_ctl_login_packet(
                    writer, False, error_msg="User not found — please register first."
                )
                return
            elif self.login_details[username] != password:
                await Packet.send_ctl_login_packet(
                    writer, False, error_msg="Incorrect password."
                )
                return
            elif any(u.username == username for u in self.users.values()):
                await Packet.send_ctl_login_packet(
                    writer,
                    False,
                    error_msg="Already logged in from another connection.",
                )
                return

            # INFO: Successfully logged in
            user = User(user_id, username)
            self.users[user_id] = user
            self.clients[user_id] = writer
            print(f"{username} logged in.")
            await Packet.send_ctl_login_packet(
                writer,
                True,
                user_id=user_id,
                members_list=self.get_members_list(),
                groups_list=self.get_groups_list(),
            )

            await self.broadcast_inf_login(user_id, username)

            # ── MAIN LOOP ─────────────────────────────────────────────────────
            while packet := await Packet.read_message(reader):
                print(packet)

                # DATA SEND
                if packet.is_data_send():
                    chat_id = packet.get_chat_id_from_data_send()
                    content = packet.get_message_from_data_send()

                    if not content:
                        await Packet.send_ctl_error_packet(
                            writer, "Message body cannot be empty."
                        )
                        continue

                    if chat_id not in self.chats:
                        # INFO: First message in a DM — validate both users exist
                        member_ids = decode_direct_chat_id(chat_id)
                        missing = [
                            uid
                            for uid in member_ids
                            if uid not in self.users and uid not in self.login_details
                        ]
                        if missing:
                            await Packet.send_ctl_error_packet(
                                writer, f"User not found: {', '.join(missing)}"
                            )
                            continue
                        self.chats[chat_id] = Chat(
                            chat_id,
                            {
                                uid: self.users[uid]
                                for uid in member_ids
                                if uid in self.users
                            },
                            True,
                        )

                    await self.broadcast_to_chat_except(chat_id, content, user_id)

                elif packet.is_file_request():
                    # Broadcast the file request to everyone in the chat
                    chat_id = packet.get_chat_id_from_file_send()

                    # INFO: Client is sending the first message in the chat
                    if chat_id not in self.chats:
                        # INFO: Assuming its a direct chat message
                        self.chats[chat_id] = Chat(
                            chat_id,
                            {
                                user_id: self.users[user_id]
                                for user_id in decode_direct_chat_id(chat_id)
                            },
                            True,
                        )

                    transfer_id = packet.get_transfer_id_from_file_send()
                    file_path = packet.get_filepath_from_file_send()
                    filename = packet.get_filename_from_file_send()
                    file_size = packet.get_filesize_from_file_send()
                    port = packet.get_port()

                    await self.broadcast_to_chat_file_except(
                        chat_id,
                        user_id,
                        transfer_id,
                        file_path,
                        filename,
                        file_size,
                        port,
                    )

                # CMD CREATE (group)
                elif packet.is_cmd_create():
                    group_name = packet.get_group_name_from_cmd_create()
                    future_id = packet.get_future_id()

                    if not group_name:
                        await Packet.send_ctl_create_packet(
                            writer, future_id, False, "Group name cannot be empty."
                        )
                    elif any(
                        c
                        for c in self.chats.values()
                        if c.group_name == group_name and not c.is_direct
                    ):
                        await Packet.send_ctl_create_packet(
                            writer,
                            future_id,
                            False,
                            f"Group name '{group_name}' is already taken.",
                        )
                    else:
                        group_chat = Chat(str(uuid.uuid4()), {}, False, group_name)
                        self.chats[group_chat.id] = group_chat
                        print(f"Group '{group_name}' created by {username}.")
                        await Packet.send_ctl_create_packet(writer, future_id, True)
                        await self.broadcast_inf_create(group_chat.id)

                # CMD JOIN (group)
                elif packet.is_cmd_join():
                    chat_id = packet.get_chat_id_from_cmd_join()
                    future_id = packet.get_future_id()

                    if chat_id not in self.chats:
                        # error, chat does not exist
                        await Packet.send_ctl_join_packet(
                            writer,
                            future_id,
                            False,
                            error_msg="This group does not exist.",
                        )
                    elif self.chats[chat_id].is_direct:
                        # error, chat is not group chat
                        await Packet.send_ctl_join_packet(
                            writer,
                            future_id,
                            False,
                            error_msg="You cannot join a direct message chat.",
                        )
                    elif user_id in self.chats[chat_id].users:
                        # error user already in this chat
                        await Packet.send_ctl_join_packet(
                            writer,
                            future_id,
                            False,
                            error_msg=f"You are already a member of '{self.chats[chat_id].group_name}'.",
                        )
                    else:
                        # Add to group, reply back to requester and announce to group of new user
                        self.chats[chat_id].users[user_id] = user
                        print(
                            f"{username} joined group '{self.chats[chat_id].group_name}'."
                        )
                        await Packet.send_ctl_join_packet(
                            writer,
                            future_id,
                            True,
                            user_ids=[
                                u_id
                                for u_id in self.chats[chat_id].users
                                if u_id != user_id
                            ],
                        )
                        await self.broadcast_inf_join(chat_id, user_id)

                # CMD LEAVE (group)
                elif packet.is_cmd_leave():
                    chat_id = packet.get_chat_id_from_cmd_leave()

                    if chat_id not in self.chats:
                        await Packet.send_ctl_error_packet(
                            writer, "This group does not exist."
                        )
                    elif self.chats[chat_id].is_direct:
                        await Packet.send_ctl_error_packet(
                            writer, "You cannot leave a direct message chat."
                        )
                    elif user_id not in self.chats[chat_id].users:
                        await Packet.send_ctl_error_packet(
                            writer, "You are not a member of this group."
                        )
                    else:
                        self.chats[chat_id].users.pop(user_id)
                        print(
                            f"{username} left group '{self.chats[chat_id].group_name}'."
                        )
                        await self.broadcast_inf_leave(chat_id, user_id)
                # CMD SEEONLINE
                elif packet.is_cmd_seeonline():
                    await Packet.send_ctl_seeonline_packet(
                        writer, self.get_members_list()
                    )

                # CMD SEEGROUPS
                elif packet.is_cmd_seegroups():
                    await Packet.send_ctl_seegroups_packet(
                        writer, self.get_groups_list()
                    )

                # Unknown packet
                else:
                    await Packet.send_ctl_error_packet(
                        writer,
                        f"Unknown message type: {packet.category} {packet.msg_type}",
                    )

        except IncompleteReadError:
            pass
        finally:
            # INFO: Server notifies clients that this client has diconnected
            if user_id in self.users:
                user = self.users.pop(user_id)
                await self.broadcast_inf_logout(user.id, user.username)
                # INFO: Close UDP connection
                self.udp_clients.pop(user.username, None)
                print(f"{user.username} disconnected.")

            # INFO: Close TCP connection
            if user_id in self.clients:
                self.clients[user_id].close()
                await self.clients[user_id].wait_closed()

            # INFO: Remove user from groups
            for chat in self.chats.values():
                chat.users.pop(user_id, None)

    def get_members_list(self) -> list[tuple[str, str]]:
        return [(u.id, u.username) for u in self.users.values()]

    def get_groups_list(self) -> list[tuple[str, str]]:
        return [
            (c.id, str(c.group_name)) for c in self.chats.values() if not c.is_direct
        ]

    async def start(self):
        """Open socket manually using sockets library"""
        # INFO: TCP socket setup
        tcp_server_sock = socket(AF_INET, SOCK_STREAM)

        tcp_server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        tcp_server_sock.bind((self.host, self.port))
        tcp_server_sock.listen()
        tcp_server_sock.setblocking(False)
        tcp_server = await asyncio.start_server(
            self.handle_client, sock=tcp_server_sock
        )

        # INFO: UDP socket setup
        self.udp_sock = socket(AF_INET, SOCK_DGRAM)
        self.udp_sock.setblocking(False)
        self.udp_sock.bind((self.host, self.port))

        print(f"[*] Serving on {self.host}:{self.port}")

        try:
            # INFO: Run both asynchronously
            await asyncio.gather(tcp_server.serve_forever(), self.run_udp_loop())
        except asyncio.CancelledError:
            pass
        finally:
            self.udp_sock.close()

    async def run_udp_loop(self):
        """UDP Server"""
        loop = asyncio.get_running_loop()
        while True:
            try:
                # INFO: Get data from UDP socket
                data, addr = await loop.sock_recvfrom(self.udp_sock, 1024)
                message = data.decode()
                parts = message.split("|")

                print(message)

                # INFO: Establish message to get the UDP port from clients
                if parts[0] == "HELLO":
                    username = parts[1]
                    self.udp_clients[username] = addr
                    print(f"HELLO {username}")

                # INFO: Typing message received
                elif parts[0] == "TYPING":
                    chat_id = parts[1]
                    sender = parts[2]
                    print(f"TYPING {chat_id} {sender}")

                    # INFO: If this chat does'nt exist, do nothing
                    if chat_id not in self.chats:
                        return

                    # INFO: Send TYPING UDP message to all relevant chat members
                    for user in self.chats[chat_id].users.values():
                        if (
                            user.username != sender
                            and user.username in self.udp_clients
                        ):
                            target_addr = self.udp_clients[user.username]
                            forward_payload = f"TYPING|{chat_id}|{sender}".encode()
                            await loop.sock_sendto(
                                self.udp_sock, forward_payload, target_addr
                            )
            except Exception as e:
                print(f"UDP Error: {e}")


if __name__ == "__main__":
    parser = ArgumentParser(description="MyChat server")
    # parser.add_argument("host", default="127.0.0.1")
    parser.add_argument("port", type=int, default=8888)
    args = parser.parse_args()

    asyncio.run(Server("127.0.0.1", args.port).start())
