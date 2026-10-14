import asyncio
import os
import uuid
from argparse import ArgumentParser
from asyncio import Future, StreamReader, StreamWriter
from datetime import datetime
from pathlib import Path
from socket import AF_INET, SOCK_DGRAM, SOCK_STREAM, socket
from typing import Any

from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.timer import Timer
from textual.widgets import (
    Button,
    ContentSwitcher,
    DirectoryTree,
    Header,
    Input,
    Label,
    ListItem,
    ListView,
    RichLog,
)

from helpers import Chat, User, decode_direct_chat_id, encode_direct_chat_id
from packet import Packet


class MainChatScreen(Screen):
    _intentional_logout = False
    pending_files: dict[str, Any] = {}
    server_addr: tuple[str, int]
    udp_sock: socket
    typing_timers: dict[str, Timer] = {}

    def __init__(
        self,
        user: User,
        reader: StreamReader,
        writer: StreamWriter,
        initial_members: list[tuple[str, str]],
        initial_groups: list[tuple[str, str]],
        server_host: str,
        server_port: int,
    ):
        super().__init__()
        self.user = user
        self.reader = reader
        self.writer = writer
        self.chats: dict[str, Chat] = {}
        self.chat_logs: dict[str, RichLog] = {}  # Stored chat histories
        self.online_users: dict[str, User] = {}
        self.active_target_id = None  # chat_id
        self.initial_members = initial_members
        self.initial_groups = initial_groups
        self.pending_requests: dict[str, Future] = {}
        self.server_addr = (server_host, server_port)

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(id="sidebar"):
                yield Label("GROUPS", classes="sidebar-label")
                yield ListView(id="group-list")
                yield Button("Create Group", id="create-group-btn", variant="primary")

                yield Label("ONLINE USERS", id="sidebar-label")
                yield ListView(id="member-list")
                yield Button("Disconnect", variant="error", id="disconnect-btn")

            with Vertical(id="chat-area"):
                with Horizontal(id="chat-header"):
                    yield Label("Select a chat", id="chat-title")
                    yield Button("Leave Group", id="leave-group-btn", variant="error")
                with ContentSwitcher(id="log-switcher"):
                    yield Label(
                        "Select a user from the sidebar to start a private conversation.",
                        id="placeholder",
                    )
                yield Label("", id="typing-indicator")
                with Horizontal(id="input-area"):
                    yield Button("📁", id="file-btn", variant="default", disabled=True)
                    yield Input(
                        placeholder="Type a message...", id="chat-input", disabled=True
                    )
                    yield Button(
                        "Send", variant="success", id="send-btn", disabled=True
                    )

    async def on_mount(self):
        """This only runs AFTER all widgets above are ready to be queried."""
        self.update_online_users(self.initial_members)
        self.update_group_list(self.initial_groups)

        self.run_worker(self.listen_for_messages())
        await self.setup_udp_listener()

        self.query_one("#chat-input", Input).focus()

    async def setup_udp_listener(self):
        self.udp_sock = socket(AF_INET, SOCK_DGRAM)
        self.udp_sock.setblocking(False)  # INFO : Critical for Textual
        self.udp_sock.bind(("0.0.0.0", 0))

        # INFO: Contact server, tell them what port the UDP socket is running on.
        loop = asyncio.get_running_loop()
        hello_msg = f"HELLO|{self.user.username}".encode()
        await loop.sock_sendto(self.udp_sock, hello_msg, self.server_addr)

        # INFO: After contacting server, start listening for messages
        self.listen_for_udp_typing()

    @work()
    async def listen_for_udp_typing(self):
        loop = asyncio.get_running_loop()
        while True:
            try:
                data, _ = await loop.sock_recvfrom(self.udp_sock, 1024)
                message = data.decode()

                if message.startswith("TYPING|"):
                    _, chat_id, sender = message.split("|")
                    self.handle_udp_typing_signal(chat_id, sender)

            except Exception:
                # INFO: Socket closed or network error
                break

    def handle_udp_typing_signal(self, chat_id: str, username: str):
        """Called when a UDP TYPING packet arrives."""

        if self.active_target_id != chat_id:
            # INFO: If I'm not looking at this chat, don't do anything
            return

        if username in self.typing_timers:
            self.typing_timers[username].reset()  # INFO: Refresh the timer if it exists
        else:
            # INFO: Create a 2-second timer. If it runs out, call _remove_typer
            self.typing_timers[username] = self.set_timer(
                delay=2.0, callback=lambda u=username: self._remove_typer(u)
            )

        self._update_typing_label()  # INFO: UI update

    def _remove_typer(self, username: str):
        """Called when a user's timer runs out. Updates the UI"""
        if username in self.typing_timers:
            del self.typing_timers[username]
            self._update_typing_label()

    def _update_typing_label(self):
        """Formats the text and handles the fade animation."""
        indicator = self.query_one("#typing-indicator", Label)
        typers = list(self.typing_timers.keys())

        if not typers:
            # INFO: Nobody is typing. Fade out.
            indicator.styles.animate("opacity", value=0.0, duration=0.5)
            return

        indicator.styles.opacity = 1.0

        if len(typers) == 1:
            text = f"{typers[0]} is typing..."
        elif len(typers) == 2:
            text = f"{typers[0]} and {typers[1]} are typing..."
        else:
            # INFO: Shows the latest typers instead of the first 1/2
            text = f"{', '.join(typers[:-1])}, and {typers[-1]} are typing..."

        indicator.update(text)

    @on(Input.Changed, "#chat-input")
    async def on_input_changed(self, event: Input.Changed):
        """Called when the chat-input changes value"""
        if event.value.strip() and self.active_target_id and self.udp_sock:
            loop = asyncio.get_running_loop()
            payload = f"TYPING|{self.active_target_id}|{self.user.username}".encode()

            await loop.sock_sendto(self.udp_sock, payload, self.server_addr)

    def on_unmount(self):
        """Called during the teardown of this screen"""
        # INFO: Close the UDP connection if its still up.
        if hasattr(self, "udp_sock"):
            self.udp_sock.close()

    def get_or_create_log(self, chat_id: str) -> RichLog:
        """Retrieves a log for a user, or creates a new one if it doesn't exist."""
        switcher = self.query_one("#log-switcher", ContentSwitcher)
        chat_name = self.chats[chat_id].get_name(self.user)
        if chat_id not in self.chat_logs:
            new_log = RichLog(id=f"log-{chat_id}", highlight=True, markup=True)
            new_log.display = False
            self.chat_logs[chat_id] = new_log
            # Adds the new log to the switcher programmatically
            switcher.mount(new_log)
            new_log.write(f"[italic gray]Beginning of chat with {chat_name}[/]")

        return self.chat_logs[chat_id]

    def get_or_create_chat(self, chat_id: str):
        if chat_id not in self.chats:
            # INFO: Assume that if we can't find the chat, its because its direct chat
            user_ids = decode_direct_chat_id(chat_id)
            self.chats[chat_id] = Chat(
                chat_id,
                {u.id: u for u in self.online_users.values() if u.id in user_ids},
                True,
            )
        return self.chats[chat_id]

    async def request_join_group(self, chat_id: str) -> "Packet":
        """Sends the CMD JOIN to SERVER and waits for the CTL using futures"""
        loop = asyncio.get_running_loop()
        future_id = str(uuid.uuid4())

        # INFO: Store future so the client can come to this point once the server replies
        future = loop.create_future()
        self.pending_requests[future_id] = future
        await Packet.send_cmd_join_packet(self.writer, chat_id, future_id)

        try:
            return await asyncio.wait_for(future, timeout=5.0)
        except asyncio.TimeoutError:
            return Packet.make_failed_ctl_join_packet("Server timed out.")
        finally:
            self.pending_requests.pop(future_id, None)

    async def on_list_view_selected(self, event: ListView.Selected):
        """Called when the user is clicks a user or group in the sidebar"""
        chat_id = event.item.id[5:]

        # INFO: Remove all typers, will get refreshed for the new chat view
        for username in self.typing_timers:
            self._remove_typer(username)

        chat: Chat = self.get_or_create_chat(chat_id)
        chat_name = chat.get_name(self.user)

        self.query_one(
            "#log-switcher", ContentSwitcher
        ).current = self.get_or_create_log(chat.id).id

        # Inside on_list_view_selected
        leave_btn = self.query_one("#leave-group-btn", Button)
        leave_btn.styles.display = "none" if chat.is_direct else "block"

        # if its a group chat and I'm not in it already
        if not chat.is_direct and self.user.id not in chat.users:
            self.query_one("#group-list", ListView).disabled = True
            self.query_one("#chat-title", Label).update(
                f"⏳ Joining [b cyan]{chat_name}[/]..."
            )
            self.disable_chat_inputs(True)

            packet: Packet = await self.request_join_group(chat_id)

            self.query_one("#group-list", ListView).disabled = False

            assert packet.is_ctl_join()

            if not packet.get_success_from_ctl_join():
                self.app.notify(
                    f"Could not join: {packet.get_error_message_from_ctl_join()}",
                    severity="error",
                )
                self.query_one("#chat-title", Label).update(
                    "[red]Failed to join group[/]"
                )
                return

            # INFO: Add user to the Group chat
            existing_user_ids = packet.get_user_ids_from_ctl_join()
            for user_id in existing_user_ids:
                self.chats[chat.id].users[user_id] = self.online_users[user_id]
            self.group_user_update(chat.id, self.user.id, self.user.username, True)

        self.active_target_id = chat.id
        self.query_one("#chat-title", Label).update(f"Chat: [b cyan]{chat_name}[/]")

        self.disable_chat_inputs(False)
        self.query_one("#chat-input", Input).focus()

    async def listen_for_messages(self):
        """Listen for TCP messages from server"""
        try:
            while packet := await Packet.read_message(self.reader):
                # INFO: If this a response from SERVER directly to another waiting fn
                if packet.is_future():
                    # INFO: Resume that function, passing the result back to it
                    self.pending_requests[packet.get_future_id()].set_result(packet)
                    # INFO: Go back to waiting for messages from SERVER
                    continue

                if packet.is_inf_login():
                    user_id, username = packet.get_user_from_inf_login()

                    self.online_users[user_id] = User(user_id, username)
                    self.append_user_to_online_users(self.online_users[user_id])
                    self.app.notify(f"{username} joined the chat!")
                elif packet.is_inf_create():
                    group_chat = packet.get_group_from_inf_create()
                    self.chats[group_chat.id] = group_chat
                    self.append_group_chat_to_group_chat_list(group_chat)
                    self.app.notify(f"Group {group_chat.group_name} has been created!")
                elif packet.is_inf_join():
                    chat_id = packet.get_chat_id_from_inf_join()
                    user_id = packet.get_user_id_from_inf_join()
                    username = packet.get_username_from_inf_join()

                    self.group_user_update(chat_id, user_id, username, True)
                elif packet.is_inf_leave():
                    chat_id = packet.get_chat_id_from_inf_leave()
                    username = packet.get_username_from_inf_leave()
                    user_id = packet.get_user_id_from_inf_leave()

                    self.group_user_update(chat_id, user_id, username, False)
                elif packet.is_inf_logout():
                    offline_id, offline_name = packet.get_user_from_inf_logout()

                    self.online_users.pop(offline_id, None)
                    self.handle_user_offline(offline_id, offline_name)
                elif packet.is_inf_data():
                    _sender_id, sender_name = packet.get_sender_user_from_inf_send()
                    chat_id = packet.get_chat_id_from_data_send()
                    message = packet.get_message_from_data_send()
                    sent_at = packet.get_sent_at()

                    self.append_message_to_chat(chat_id, message, sent_at, sender_name)
                    self._remove_typer(sender_name)

                elif packet.is_file_response():
                    filename = packet.get_filename_from_file_send()
                    transfer_id = packet.get_transfer_id_from_file_send()
                    _sid, sender_name = packet.get_sender_user_from_file_send()
                    chat_id = packet.get_chat_id_from_file_send()

                    self.pending_files[transfer_id] = packet.headers

                    sent_at = packet.get_sent_at()
                    self.append_message_to_chat(
                        chat_id, filename, sent_at, sender_name, transfer_id
                    )

                elif packet.is_ctl_error():
                    error_message = packet.get_error_message_from_ctl_error()
                    self.notify(f"Error: {error_message}", severity="error")

        except (ConnectionResetError, asyncio.IncompleteReadError):
            # INFO: The server has crashed or client has logged on
            pass
        except Exception:
            self.app.notify("Unxpected error.", severity="error")
        finally:
            if not self._intentional_logout:
                self.app.notify("Server connection lost.", severity="error")

                # Forcefully close our side of the broken socket
                self.writer.close()
                await self.writer.wait_closed()

                # Kick the user back to the login screen
                self.app.pop_screen()

    def append_message_to_chat(
        self,
        chat_id: str,
        message: str,
        sent_at: datetime,
        sender_name="",
        transfer_id: str = "",
    ):
        """Helper method to append all messages (SEND/MEDIA) to any a chat log (DIRECT/GROUP)"""
        self.get_or_create_chat(chat_id)
        log = self.get_or_create_log(chat_id)

        rich_message_components = [
            (f"[{sent_at.strftime('%H:%M:%S')}] ", "dim"),
            ("Me: ", "bold green"),
            (message, "white"),
        ]

        if sender_name:
            rich_message_components[1] = (f"{sender_name}: ", "b cyan")

        text = Text.assemble(*rich_message_components)

        # ISSUE: I couldn't get this to work with Text.assemble, but it should work
        if sender_name and transfer_id:
            timestamp = f"[dim][{sent_at.strftime('%H:%M:%S')}][/] "
            text2 = f"[@click=screen.click_file('{transfer_id}')][u]{message}[/u][/]"
            text = Text.from_markup(f"{timestamp}[b cyan]{sender_name}:[/] {text2}")

        log.write(text, scroll_end=True)

        if self.active_target_id != chat_id and sender_name:
            self.app.notify(f"New message from {sender_name}")

    def group_user_update(self, chat_id: str, user_id: str, username: str, join: bool):
        """Helper function to notify user when another user leaves/joins a group."""
        if join:
            self.chats[chat_id].users[user_id] = self.online_users[user_id]
            if user_id != self.user.id:
                self.app.notify(
                    f"{username} has joined {self.chats[chat_id].group_name}!"
                )
        else:
            self.chats[chat_id].users.pop(user_id, None)
            if user_id != self.user.id:
                self.app.notify(f"{username} has left {self.chats[chat_id].group_name}")

    def update_online_users(self, members: list[tuple[str, str]]) -> None:
        """Called only once when the user logs in."""
        self.online_users = {u[0]: User(u[0], u[1]) for u in members}

        # clear the ui
        self.query_one("#member-list", ListView).clear()

        # Rebuild the ui
        for user in self.online_users.values():
            self.append_user_to_online_users(user)

    def append_user_to_online_users(self, user: User):
        """Helper function to append a new user to the members list"""
        list_view = self.query_one("#member-list", ListView)
        if user.id != self.user.id:
            item = ListItem(
                Label(f"👤 {user.username}"),
                id=f"chat-{encode_direct_chat_id(self.user.id, user.id)}",
            )
            list_view.append(item)

    def update_group_list(self, groups: list[tuple[str, str]]) -> None:
        """Refreshes the Groups section of the sidebar."""

        # ISSUE: This function should only be called when use logs in
        # It will purge all chats, including direct chats
        self.chats = {c[0]: Chat(c[0], {}, False, c[1]) for c in groups}

        # NOTE: In the future, we will add/remove 1 group at a time like users, this is easier for now
        self.query_one("#group-list", ListView).clear()

        for chat in self.chats.values():
            if not chat.is_direct:
                self.append_group_chat_to_group_chat_list(chat)

    def append_group_chat_to_group_chat_list(self, group_chat: Chat):
        self.query_one("#group-list", ListView).append(
            ListItem(Label(f"📁 {group_chat.group_name}"), id=f"chat-{group_chat.id}")
        )

    @work()
    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "disconnect-btn":
            await self.exit_chat()
        elif event.button.id == "send-btn":
            await self.process_send()
        elif event.button.id == "create-group-btn":
            self.app.push_screen(CreateGroupModal(self))
        elif event.button.id == "leave-group-btn":
            await self.leave_group()
        elif event.button.id == "file-btn":
            await self.upload_media_to_chat()

    """File transfer logic"""

    async def upload_media_to_chat(self):
        assert self.active_target_id is not None

        file_path: Path = await self.app.push_screen_wait(FilePickerModal())

        if file_path:
            self.run_worker(self.initiate_transfer_server(str(file_path.absolute())))

    async def initiate_transfer_server(self, file_path: str):
        """Establish new dedicated P2P TCP socket for file download"""

        server = await asyncio.start_server(
            lambda r, w: self.send_file_data(w, file_path), "127.0.0.1", 0
        )
        port = server.sockets[0].getsockname()[1]

        # Close server after a timeout
        asyncio.create_task(self.close_p2p_server(server))

        await self.process_file_send(file_path, port)

    async def send_file_data(self, writer, path):
        """Sender streams bits to the recipient."""
        try:
            with open(path, "rb") as f:
                while chunk := f.read(8192):
                    writer.write(chunk)
                    await writer.drain()
        except Exception as e:
            self.app.notify(f"P2P Error: {e}", severity="error")
        finally:
            writer.close()

    async def action_click_file(self, transfer_id: str):
        """Recipient connects to Sender's IP:Port."""
        info = self.pending_files.get(transfer_id)
        if not info:
            return

        try:
            # INFO: Connect to the sender's P2P port for this file download
            reader, writer = await asyncio.open_connection(
                "127.0.0.1", int(info["port"])
            )
            save_path = os.path.join(os.getcwd(), f"recv_{info['filename']}")

            # INFO: Save TCP chunks to save_path (downloading file from uploader)
            with open(save_path, "wb") as f:
                while chunk := await reader.read(4096):
                    f.write(chunk)

            self.app.notify(f"File saved to {save_path}")
            writer.close()
        except Exception as e:
            self.app.notify(f"Transfer failed: {e}", severity="error")

    async def close_p2p_server(self, server: asyncio.Server):
        """Automatically shuts down the P2P server after a timeout."""
        # We give the recipient 1 hour to download file
        await asyncio.sleep(3600)
        server.close()
        await server.wait_closed()

    async def process_file_send(self, file_path: str, port: str):
        """Sends DATA SEND to server. The receiver can chose to download the file or not."""
        filename = os.path.basename(file_path)
        filesize = os.path.getsize(file_path)
        transfer_id = str(uuid.uuid4())[:6]

        if self.active_target_id:
            self.get_or_create_chat(self.active_target_id)

            await Packet.send_file_request_packet(
                self.writer,
                self.active_target_id,
                transfer_id,
                file_path,
                filename,
                filesize,
                port,
            )

            sent_at = datetime.now()
            self.append_message_to_chat(self.active_target_id, filename, sent_at, "")

    async def leave_group(self):
        assert self.active_target_id is not None

        # TODO: We ought to handle for errors like we do for join group.
        # This assumes that the leave is successful (most of the time it is).
        await Packet.send_cmd_leave_packet(self.writer, self.active_target_id)

        self.reset_chat_ui()

        # INFO: Remove myself from that group
        self.group_user_update(
            self.active_target_id, self.user.id, self.user.username, False
        )

        self.app.notify(f"Left group: {self.chats[self.active_target_id].group_name}")

    def reset_chat_ui(self):
        self.query_one("#chat-title", Label).update("Select a chat...")
        self.query_one("#leave-group-btn", Button).styles.display = "none"
        self.disable_chat_inputs(True)

    def disable_chat_inputs(self, disabled: bool = True):
        self.query_one("#chat-input", Input).disabled = disabled
        self.query_one("#send-btn", Button).disabled = disabled
        self.query_one("#file-btn", Button).disabled = disabled
        self.query_one("#leave-group-btn", Button).disabled = disabled

    def handle_user_offline(self, user_id: str, username: str):
        chat_id = encode_direct_chat_id(user_id, self.user.id)

        # Remove user from members-list
        list_view = self.query_one("#member-list", ListView)
        item_to_remove = next(
            item for item in list_view.children if item.id[5:] == chat_id
        )
        if item_to_remove:
            item_to_remove.remove()

        # If we are currently talking to them, disable the input
        if self.active_target_id == chat_id:
            self.active_target_id = None
            self.disable_chat_inputs()
            chat_input = self.query_one("#chat-input", Input)

            chat_input.placeholder = "User went offline..."
            self.query_one("#chat-title", Label).update("[red]User is offline[/]")

        # Append chat if you were chatting with offline user
        if chat_id in self.chat_logs:
            self.chat_logs[chat_id].write(
                f"\n[bold red]*** {username} has disconnected. ***[/]"
            )

        # Toast message
        self.app.notify(f"{username} logged out.", severity="warning")

    async def on_input_submitted(self):
        """Called when user clicks enter will input is focused"""
        await self.process_send()

    async def process_send(self):
        """Sends DATA SEND to server. Works for Group/Direct chats"""
        chat_input = self.query_one("#chat-input", Input)
        message = chat_input.value
        if message and self.active_target_id:
            chat = self.get_or_create_chat(self.active_target_id)

            # Send the routed message
            sent_at = datetime.now()
            await Packet.send_data_send_packet(self.writer, chat.id, message)
            self.append_message_to_chat(chat.id, message, sent_at)

            chat_input.value = ""
            chat_input.focus()

    async def request_create_group(self, group_name: str) -> "Packet":
        """Helper function for CMD CREATE"""
        loop = asyncio.get_running_loop()
        future_id = str(uuid.uuid4())

        # INFO: Create a future and store it in pending_requests
        future = loop.create_future()
        self.pending_requests[future_id] = future

        await Packet.send_cmd_create_packet(self.writer, group_name, future_id)

        try:
            # INFO: Wait for the background loop to resolve this future
            # Use wait_for so the UI doesn't hang forever if the server drops it
            return await asyncio.wait_for(future, timeout=5.0)
        except asyncio.TimeoutError:
            return Packet.make_failed_ctl_create_packet("Server timed out.")
        finally:
            # INFO: This block will always run, even if a value is returned in the try/except
            self.pending_requests.pop(future_id, None)

    async def exit_chat(self):
        """Clean up and return to LoginScreen."""
        self._intentional_logout = True

        # INFO: SERVER will inform other clients of the logout event
        self.writer.close()
        await self.writer.wait_closed()

        self.app.notify("Disconnected.", severity="information")

        self.app.pop_screen()


class ChatApp(App):
    CSS = """
    Center { height: 100%; }
    Middle { width: 40; border: thick $primary; padding: 2; }

    MainChatScreen {
        #sidebar { width: 30; border-right: solid $primary; padding: 1; background: $panel; }
        #sidebar-title { text-style: bold; margin-bottom: 1; text-align: center; }
        ListItem.--highlight { background: $accent; color: $text; text-style: bold; }

        #chat-header {height: 3; background: $boost; align: left middle; padding: 0 1;}
        #chat-title {width: 1fr; content-align: left middle;}
        #leave-group-btn {display: none; min-width: 12;}
        #chat-area { width: 1fr; }
        #log-switcher { height: 1fr; margin-bottom: 1; }
        #input-area { height: auto; dock: bottom; padding: 0 1; }
        #chat-input { width: 1fr; /* Takes up all available space */ }
        Input:disabled { background: $surface; color: $text-muted; border: tall $background; }
        #send-btn { width: 10; margin-left: 1; min-width: 8; }
        #send-btn.-success { background: $success; color: $text; }
    }

    WelcomeScreen, LoginScreen, RegisterScreen, CreateGroupModal {
        align: center middle;
        background: $background;

        #setup-container { width: 40; height: auto; border: round $primary; background: $panel; padding: 1 2; content-align: center middle; }
        .title { width: 100%; content-align: center middle; text-style: bold; color: $accent; margin-bottom: 1; }
        Button { 
            width: 100%; margin-top: 1; 
        }
        Input { margin-bottom: 1; border: round $primary-lighten-2; background: $panel }
    }
    """
    server_host: str
    server_port: int

    def __init__(self, server_host: str, server_port: int):
        super().__init__()
        self.server_host = server_host
        self.server_port = server_port

    def on_mount(self):
        self.push_screen(WelcomeScreen())

    async def connect_and_switch(self, username: str, password: str, login: bool):
        """Handle the socket connection and then swap screens."""
        try:
            # INFO: Necessary for asyncio and by extension Textual to work
            loop = asyncio.get_running_loop()

            client_socket = socket(AF_INET, SOCK_STREAM)  # INFO: Make socket
            client_socket.setblocking(False)  # INFO: Important for Textual UI
            # INFO: Connect in the background
            await loop.sock_connect(client_socket, (self.server_host, self.server_port))

            # INFO: StreamReader, StreamWriter asyncio Wrappers
            reader, writer = await asyncio.open_connection(sock=client_socket)

            # Send Request
            if login:
                await Packet.send_cmd_login_packet(writer, username, password)
            else:
                await Packet.send_cmd_register_packet(writer, username, password)

            packet = await Packet.read_message(reader)

            if packet.is_empty:
                self.notify("Server closed connection.", severity="error")
                return

            if packet.is_ctl_login():
                if packet.get_success_from_ctl_login():
                    # INFO: Successful login, swtich to MainChatScreen
                    user_id = packet.get_user_id_from_ctl_login()
                    members_list = packet.get_members_list_from_ctl_login()
                    groups_list = packet.get_groups_list_from_ctl_login()

                    chat_screen = MainChatScreen(
                        User(user_id, username),
                        reader,
                        writer,
                        members_list,
                        groups_list,
                        self.server_host,
                        self.server_port,
                    )
                    self.app.notify("Logged in Successfully!")
                    await self.push_screen(chat_screen)
                else:
                    # INFO: Failed login, user gets feedback
                    error_message = packet.get_error_message_from_ctl_login()
                    self.notify(f"Login Failed: {error_message}", severity="error")

                    writer.close()
                    await writer.wait_closed()

            elif packet.is_ctl_register():
                if packet.get_success_from_ctl_register():
                    # INFO: Successful register, user is sent to WelcomeScreen
                    self.app.notify("Registered successfully! Please log in.")
                    self.app.pop_screen()  # Back to WelcomeScreen
                else:
                    # INFO: Failed register, user gets feedback
                    error_message = packet.get_error_message_from_ctl_registration()
                    self.notify(
                        f"Registration Failed: {error_message}", severity="error"
                    )
                writer.close()
                await writer.wait_closed()

            elif packet.is_ctl_error():
                # INFO: General error from the server
                error_message = packet.get_error_message_from_ctl_error()
                self.notify(f"Error: {error_message}", severity="error")
                writer.close()
                await writer.wait_closed()

        except ConnectionRefusedError:
            # INFO: You may have entered the wrong IP/port for the server
            self.notify("Could not connect: Server is offline.", severity="error")


class FilePickerModal(ModalScreen[Path]):
    """A native Textual file picker modal."""

    def compose(self) -> ComposeResult:
        # INFO: Start the tree in the user's home directory
        yield DirectoryTree(str(Path.home()), id="file-tree")

    def on_directory_tree_file_selected(
        self, event: DirectoryTree.FileSelected
    ) -> None:
        """Triggered when the user hits Enter on a file."""
        self.dismiss(event.path)  # INFO: Return the selected Path to the main screen


class WelcomeScreen(Screen):
    """First screen the user sees, has buttons leading to other screens."""

    def compose(self) -> ComposeResult:
        with Vertical(id="setup-container"):
            yield Label("Welcome to my chat app", classes="title")

            yield Button("Login", id="welcome-login-btn", variant="primary")
            yield Button("Register", id="welcome-register-btn")

            yield Button(
                "Exit", classes="back-btn", id="welcome-exit-btn", variant="error"
            )

    async def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "welcome-login-btn":
            await self.app.push_screen(LoginScreen())
        elif event.button.id == "welcome-register-btn":
            await self.app.push_screen(RegisterScreen())
        elif event.button.id == "welcome-exit-btn":
            self.app.exit()


class RegisterScreen(Screen):
    app: "ChatApp"

    def compose(self) -> ComposeResult:
        with Vertical(id="setup-container"):
            yield Label("Register", classes="title")
            yield Input(placeholder="Username", id="username-input")
            yield Input(placeholder="Password", id="password-input", password=True)
            yield Button("Register", variant="primary", id="register-btn")

            yield Button("Back", classes="back-btn", id="register-back-btn")

    async def on_input_submitted(self):
        """Called when the user presses enter while focus is on the input element."""
        await self.attempt_register()

    async def on_button_pressed(self, event: Button.Pressed):
        """Called when any button is pressed on this screen."""
        if event.button.id == "register-back-btn":
            self.dismiss()
        elif event.button.id == "register-btn":
            await self.attempt_register()

    async def attempt_register(self):
        """Attempt to register user with server."""
        username = self.query_one("#username-input", Input).value
        password = self.query_one("#password-input", Input).value

        if not username:
            self.app.notify("Username field is empty.", severity="error")
            return

        await self.app.connect_and_switch(username, password, False)


class LoginScreen(Screen):
    app: "ChatApp"

    def compose(self) -> ComposeResult:
        with Vertical(id="setup-container"):
            yield Label("Login", classes="title")

            yield Input(placeholder="Username", id="username-input")
            yield Input(placeholder="Password", id="password-input", password=True)
            yield Button("Login", variant="primary", id="login-btn")

            yield Button("Back", classes="back-btn", id="login-back-btn")

    async def on_input_submitted(self):
        """Called when the user presses enter while focus is on the input element."""
        await self.attempt_login()

    async def on_button_pressed(self, event: Button.Pressed):
        """Called when any button is pressed on this screen."""
        if event.button.id == "login-back-btn":
            self.dismiss()
        elif event.button.id == "login-btn":
            await self.attempt_login()

    async def attempt_login(self):
        username = self.query_one("#username-input", Input).value
        password = self.query_one("#password-input", Input).value

        if not username:
            self.app.notify("Username field is empty.", severity="error")
            return

        await self.app.connect_and_switch(username, password, True)


class CreateGroupModal(ModalScreen[None]):
    """Popup for creating a new group"""

    app: "MainChatScreen"

    def __init__(self, chat_screen: "MainChatScreen"):
        super().__init__()
        self.chat_screen = chat_screen

    def compose(self) -> ComposeResult:
        with Vertical(id="setup-container"):
            yield Label("Create a New Public Group", classes="title")
            yield Input(placeholder="Group name", id="group-name-input")

            yield Button("Create", variant="success", id="confirm-group-btn")
            yield Button("Cancel", variant="error", id="cancel-group-btn")

    def on_mount(self) -> None:
        self.query_one("#group-name-input", Input).focus()

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel-group-btn":
            self.dismiss(None)  # Close and return to MainChatScreen
        elif event.button.id == "confirm-group-btn":
            await self.action_submit()

    async def on_input_submitted(self) -> None:
        await self.action_submit()

    async def action_submit(self) -> None:
        """Attempt to create new group."""
        group_name_input = self.query_one("#group-name-input", Input)
        confirm_btn = self.query_one("#confirm-group-btn", Button)
        cancel_btn = self.query_one("#cancel-group-btn", Button)

        group_name = group_name_input.value.strip()
        if not group_name:
            # TODO: Should rather use Textual's builtin validation than send a toast
            self.app.notify("Group name cannot be empty!", severity="warning")
            return

        group_name_input.disabled = True
        confirm_btn.disabled = True
        cancel_btn.disabled = True
        confirm_btn.label = "Waiting..."

        # INFO: Makes use of futures to get feeback for this exact CMD
        packet: Packet = await self.chat_screen.request_create_group(group_name)

        if packet.is_ctl_create_success():
            # INFO: Client will soon receive the announcement from SERVER that a new group was created
            self.dismiss()
        else:
            self.app.notify(
                f"Error: {packet.get_error_msg_from_ctl_create()}", severity="error"
            )
            group_name_input.disabled = False
            confirm_btn.disabled = False
            cancel_btn.disabled = False
            confirm_btn.label = "Create"
            group_name_input.focus()


if __name__ == "__main__":
    parser = ArgumentParser(description="MyChat server")
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    args = parser.parse_args()

    ChatApp(args.host, args.port).run()
