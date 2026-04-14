import argparse
import csv
import socket
import select
from dataclasses import dataclass
from typing import Optional, Dict, List

DEFAULT_HOST = "192.168.20.11"
DEFAULT_PORT = 7481
DEFAULT_TIMEOUT = 5.0

DEFAULT_TX_CONFIG: Dict[str, object] = {
    "CHAN": 1,
    "BW": "20M",
    "OFFSET": 0,
    "MODE": "DSSS",
    "OFDM_MODE": "MM",
    "RATE": "1M",
    "CODING": "BCC",
    "DUTY_CYCLE": 100,
    "PSDU_LEN": 10000,
}

KEY_ALIASES = {
    "CHAN": "CHAN",
    "CH": "CHAN",
    "BW": "BW",
    "OFFSET": "OFFSET",
    "MODE": "MODE",
    "OFDM MODE": "OFDM_MODE",
    "OFDM_MODE": "OFDM_MODE",
    "RATE": "RATE",
    "CODING": "CODING",
    "DUTY CYCLE": "DUTY_CYCLE",
    "DUTY_CYCLE": "DUTY_CYCLE",
    "PSDU LEN": "PSDU_LEN",
    "PSDU_LEN": "PSDU_LEN",
}


@dataclass
class TxConfig:
    chan: int
    bw: str
    offset: float
    mode: str
    ofdm_mode: str
    rate: str
    coding: str
    duty_cycle: int
    psdu_len: int


@dataclass
class RxConfig:
    chan: int
    bw: str
    offset: float


class GuiSocketClient:
    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        timeout: float = DEFAULT_TIMEOUT,
        trailing_space: bool = False,
        log_io: bool = False,
    ):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.trailing_space = trailing_space
        self.log_io = log_io
        self._sock: Optional[socket.socket] = None

    def connect(self) -> None:
        if self._sock:
            return
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect((self.host, self.port))
        self._sock = sock

    def close(self) -> None:
        if not self._sock:
            return
        try:
            self._sock.close()
        finally:
            self._sock = None

    def _send(self, cmd: str) -> None:
        if not self._sock:
            raise RuntimeError("Socket not connected")
        self._drain_recv()
        clean = cmd.rstrip("\r\n")
        suffix = " " if self.trailing_space else ""
        data = (clean + suffix + "\r\n").encode("ascii")
        if self.log_io:
            print(f"[GUI SEND] {data!r}")
        self._sock.sendall(data)

    def _recv_line(self) -> str:
        if not self._sock:
            raise RuntimeError("Socket not connected")
        chunks = []
        wait_timeout = self.timeout
        while True:
            readable, _, _ = select.select([self._sock], [], [], wait_timeout)
            if not readable:
                break
            chunk = self._sock.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
            wait_timeout = 0.2
        data = b"".join(chunks)
        if self.log_io and data:
            print(f"[GUI RECV] {data!r}")
        return data.decode(errors="ignore").strip()

    def _drain_recv(self) -> None:
        if not self._sock:
            return
        while True:
            readable, _, _ = select.select([self._sock], [], [], 0)
            if not readable:
                break
            chunk = self._sock.recv(4096)
            if not chunk:
                break

    def send(self, cmd: str) -> None:
        self._send(cmd)

    def query(self, cmd: str) -> str:
        self._send(cmd)
        return self._recv_line()

    # Commands
    def connect_type(self, connect_type: str) -> None:
        clean = connect_type.strip().upper()
        if not clean:
            raise ValueError("connect_type is empty")
        self.send(f"CONNECT TYPE {clean}")

    def connect_usb(self) -> None:
        self.connect_type("USB")

    def connect_i2c(self) -> None:
        self.connect_type("I2C")

    def disconnect(self) -> None:
        self.send("DISCONNECT")

    def tx_config(self, cfg: TxConfig) -> None:
        cmd = (
            "TX_CONFIG "
            f"CHAN {cfg.chan} "
            f"BW {cfg.bw} "
            f"OFFSET {cfg.offset} "
            f"MODE {cfg.mode} "
            f"OFDM_MODE {cfg.ofdm_mode} "
            f"RATE {cfg.rate} "
            f"CODING {cfg.coding} "
            f"DUTY_CYCLE {cfg.duty_cycle} "
            f"PSDU_LEN {cfg.psdu_len}"
        )
        self.send(cmd)

    def start_tx(self) -> None:
        self.send("START_TX")

    def stop_tx(self) -> None:
        self.send("STOP_TX")

    def rx_config(self, cfg: RxConfig) -> None:
        cmd = f"RX_CONFIG CHAN {cfg.chan} BW {cfg.bw} OFFSET {cfg.offset}"
        self.send(cmd)

    def start_rx(self) -> None:
        self.send("START_RX")

    def stop_rx(self) -> None:
        self.send("STOP_RX")

    def write_dcxo(self, dcxo: int) -> None:
        self.send(f"WRITE_DCXO DCXO {dcxo}")

    def read_dcxo(self) -> str:
        return self.query("READ_DCXO")

    def power_target(self, power: float) -> None:
        self.send(f"POWER_TARGET POWER {power}")

    def power_get(self) -> str:
        return self.query("POWER_GET")

    def bt_power_get(self) -> str:
        return self.query("BT_POWER_GET")

    def select_firmware(self, pathfile: str) -> None:
        clean = pathfile.strip()
        if not clean:
            raise ValueError("pathfile is empty")
        self.send(f"SELECT_FIRMWARE PATHFILE {clean}")

    def antenna(self, ant: str) -> None:
        clean = ant.strip().upper()
        if clean not in {"ANT1", "ANT2", "ALL"}:
            raise ValueError("ant must be ANT1, ANT2, or ALL")
        self.send(f"ANTENNA ANT {clean}")

    def certification(self, mode: str) -> None:
        clean = mode.strip().upper()
        if clean not in {"NORMAL", "CE", "FCC", "SRRC", "SRRC_2"}:
            raise ValueError("mode must be NORMAL, CE, FCC, SRRC, or SRRC_2")
        self.send(f"CERTIFICATION MODE {clean}")

    def get_version(self) -> str:
        return self.query("GET_VERSION")


HELP_TEXT = """
Commands:
  CONNECT TYPE USB|I2C
  DISCONNECT
  TX_CONFIG CHAN <n> BW <bw> OFFSET <n> MODE <m> OFDM_MODE <m> RATE <r> CODING <c> DUTY_CYCLE <n> PSDU_LEN <n>
  START_TX | STOP_TX
  RX_CONFIG CHAN <n> BW <bw> OFFSET <n>
  START_RX | STOP_RX
  WRITE_DCXO DCXO <n>
  READ_DCXO
  POWER_TARGET POWER <float>
  POWER_GET
  BT_POWER_GET
  SELECT_FIRMWARE PATHFILE <path>
  ANTENNA ANT <ANT1/ANT2/ALL>
  CERTIFICATION MODE <NORMAL/CE/FCC/SRRC/SRRC_2>
  GET_VERSION

Local commands:
  help    show this help
  quit    exit
""".strip()


def run_repl(client: GuiSocketClient) -> None:
    print("Connected. Type 'help' for commands.")
    while True:
        try:
            line = input("gui> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not line:
            continue
        cmd_upper = line.strip().upper()
        if cmd_upper in {"QUIT", "EXIT"}:
            break
        if cmd_upper == "HELP":
            print(HELP_TEXT)
            continue

        first = cmd_upper.split()[0]
        try:
            if first in {"READ_DCXO", "POWER_GET", "BT_POWER_GET", "GET_VERSION"}:
                resp = client.query(line)
                print(resp)
            else:
                client.send(line)
        except Exception as exc:
            print(f"[ERROR] {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GUI TCP control REPL")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--extract-csv", dest="extract_csv", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.extract_csv:
        cmds = extract_tx_commands_from_csv(args.extract_csv)
        for cmd in cmds:
            print(cmd)
        return

    client = GuiSocketClient(args.host, args.port, args.timeout)
    try:
        client.connect()
        run_repl(client)
    finally:
        client.close()


if __name__ == "__main__":
    main()


def _normalize_key(key: str) -> Optional[str]:
    cleaned = key.strip().upper().replace("_", " ")
    return KEY_ALIASES.get(cleaned)


def _parse_chan(value: str) -> Optional[int]:
    raw = value.strip()
    if not raw:
        return None
    upper = raw.upper()
    if upper.startswith("CH"):
        raw = raw[2:]
    try:
        return int(float(raw))
    except ValueError:
        return None


def _parse_config_cell(cell: str, out: Dict[str, object]) -> None:
    if ":" not in cell:
        return
    key, value = cell.split(":", 1)
    norm = _normalize_key(key)
    if not norm:
        return
    val = value.strip()
    if not val:
        return
    if norm == "CHAN":
        chan = _parse_chan(val)
        if chan is not None:
            out[norm] = chan
        return
    if norm == "OFFSET":
        try:
            out[norm] = float(val)
        except ValueError:
            pass
        return
    if norm in {"DUTY_CYCLE", "PSDU_LEN"}:
        try:
            out[norm] = int(float(val))
        except ValueError:
            pass
        return
    out[norm] = val


def _build_tx_config_cmd(config: Dict[str, object]) -> str:
    return (
        "TX_CONFIG "
        f"CHAN {config['CHAN']} "
        f"BW {config['BW']} "
        f"OFFSET {config['OFFSET']} "
        f"MODE {config['MODE']} "
        f"OFDM_MODE {config['OFDM_MODE']} "
        f"RATE {config['RATE']} "
        f"CODING {config['CODING']} "
        f"DUTY_CYCLE {config['DUTY_CYCLE']} "
        f"PSDU_LEN {config['PSDU_LEN']}"
    )


def extract_tx_commands_from_csv(path: str) -> List[str]:
    commands: List[str] = []
    defaults = dict(DEFAULT_TX_CONFIG)
    header: List[str] = []

    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or all(not c.strip() for c in row):
                continue

            if any(":" in c for c in row):
                defaults = dict(DEFAULT_TX_CONFIG)
                for cell in row:
                    _parse_config_cell(cell, defaults)
                header = []
                continue

            first = row[0].strip().upper()
            if first == "CH":
                header = [c.strip() for c in row]
                continue

            if not header:
                continue

            row_map = dict(zip(header, row))
            config = dict(defaults)
            for key, value in row_map.items():
                if not value.strip():
                    continue
                norm = _normalize_key(key)
                if not norm:
                    continue
                if norm == "CHAN":
                    chan = _parse_chan(value)
                    if chan is not None:
                        config[norm] = chan
                    continue
                if norm == "OFFSET":
                    try:
                        config[norm] = float(value)
                    except ValueError:
                        pass
                    continue
                if norm in {"DUTY_CYCLE", "PSDU_LEN"}:
                    try:
                        config[norm] = int(float(value))
                    except ValueError:
                        pass
                    continue
                config[norm] = value.strip()

            commands.append(_build_tx_config_cmd(config))

    return commands
