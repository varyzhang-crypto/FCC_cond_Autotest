from typing import Any, Callable, Dict, List, Literal, Optional, Protocol, Tuple


WIFI_BANDEDGE_COLUMNS = ("BandEdgeL", "BandEdgeU")


class ScpiLike(Protocol):
    def send_cmd(self, cmd: str, read_reply: bool = False, bufsize: int = 8192):
        ...

    def query_float(self, cmd: str, bufsize: int = 8192) -> float:
        ...

    def check_error(self, label: str = "") -> str:
        ...


def _lookup_cable_loss_db(
    freq_hz: float,
    table: List[Tuple[float, float, float]],
) -> Optional[float]:
    for start_hz, end_hz, loss_db in table:
        if start_hz <= freq_hz < end_hz:
            return loss_db
    for start_hz, end_hz, loss_db in table:
        if abs(freq_hz - end_hz) < 1e-6:
            return loss_db
    return None


def _wifi_bandedge_targets(f0_hz: float) -> List[Tuple[str, float]]:
    # 2.4G and 5G bandedge points use different fixed band edges.
    if f0_hz < 3e9:
        return [("BandEdgeL", 2400e6), ("BandEdgeU", 2483.5e6)]
    if f0_hz >= 4e9:
        return [("BandEdgeL", 5150e6), ("BandEdgeU", 5850e6)]
    return []


def _measure_one_bandedge(
    inst: ScpiLike,
    ensure_fsv_initialized: Callable[[ScpiLike], None],
    f_center_hz: float,
    att_list: List[int],
    span_hz: float = 20e6,
    ref_level_dbm: float = 25.0,
    loss_table: Optional[List[Tuple[float, float, float]]] = None,
) -> Dict[str, Any]:
    results: List[Dict[str, float]] = []
    ensure_fsv_initialized(inst)
    inst.send_cmd("*CLS")
    inst.send_cmd("SYST:DISP:UPD ON")
    inst.send_cmd("SENS:FREQ:MODE SWE")
    inst.send_cmd("INIT:CONT OFF")
    inst.send_cmd(f"SENS:FREQ:CENT {f_center_hz:.0f}")
    inst.send_cmd(f"SENS:FREQ:SPAN {int(span_hz)}")
    inst.send_cmd("SENS:BAND:AUTO OFF")
    inst.send_cmd("SENS:BAND 1MHz")
    inst.send_cmd("SENS:BAND:VID:AUTO OFF")
    inst.send_cmd("SENS:BAND:VID 1MHz")
    inst.send_cmd("DISP:WIND:SUBW:TRAC1:MODE AVER")
    inst.send_cmd("SENS:WIND:DET1:FUNC RMS")
    inst.send_cmd("SENS:AVER:TYPE POW")
    inst.send_cmd("SENS:AVER:COUN 100")
    inst.send_cmd(f"DISP:WIND:TRAC:Y:SCAL:RLEV {ref_level_dbm}")
    if loss_table:
        loss_db = _lookup_cable_loss_db(f_center_hz, loss_table)
        if loss_db is not None:
            inst.send_cmd(f"DISP:WIND:TRAC:Y:SCAL:RLEV:OFFS {loss_db}")
    inst.send_cmd("INP:ATT:AUTO OFF")
    inst.send_cmd("CALC1:MARK1:STAT ON")

    for att in att_list:
        inst.send_cmd(f"INP:ATT {att}")
        inst.send_cmd("INIT:IMM;*WAI")
        inst.send_cmd("CALC1:MARK1:MAX")
        x_hz = inst.query_float("CALC1:MARK1:X?")
        y_dbm = inst.query_float("CALC1:MARK1:Y?")
        inst.check_error(f"bandedge {f_center_hz/1e6:.1f}MHz, ATT={att}")
        results.append({"att": float(att), "freq_hz": x_hz, "power": y_dbm})
    best = max(results, key=lambda item: item["power"])
    return {"center_hz": f_center_hz, "trials": results, "best": best}


def measure_wifi_bandedges(
    inst: ScpiLike,
    f0_hz: float,
    ensure_fsv_initialized: Callable[[ScpiLike], None],
    loss_table: Optional[List[Tuple[float, float, float]]] = None,
) -> Dict[str, float]:
    targets = _wifi_bandedge_targets(f0_hz)
    if not targets:
        return {}
    out: Dict[str, float] = {}
    for label, center_hz in targets:
        result = _measure_one_bandedge(
            inst,
            ensure_fsv_initialized,
            center_hz,
            [25, 20, 15],
            loss_table=loss_table,
        )
        out[label] = float(result["best"]["power"])
    return out


def run_bandedge_test(
    run_csv_test_fn: Callable[..., None],
    input_path: str,
    output_path: str,
    inst: ScpiLike,
    gui,
    default_connect_type: Optional[str] = None,
    default_firmware_type: Optional[str] = "WIFI",
    cable_loss_table: Optional[List[Tuple[float, float, float]]] = None,
    band_24: bool = True,
    band_5: bool = True,
    flow_mode: str = "WIFI",
) -> None:
    # Bandedge is an independent task: no scope sweep, no harmonic output.
    run_csv_test_fn(
        input_path=input_path,
        output_path=output_path,
        inst=inst,
        gui=gui,
        default_connect_type=default_connect_type,
        default_firmware_type=default_firmware_type,
        cable_loss_table=cable_loss_table,
        cal_scope_min=None,
        cal_scope_max=None,
        cal_scope_step=None,
        band_24=band_24,
        band_5=band_5,
        test_harmonic=False,
        test_bandedge=True,
        flow_mode=flow_mode,
    )


def measure_bandedge_side_max(
    inst: ScpiLike,
    ensure_fsv_initialized: Callable[[ScpiLike], None],
    edge_hz: float,
    side: Literal["LEFT", "RIGHT"],
    span_hz: float = 20e6,
    ref_level_dbm: float = 25.0,
    loss_table: Optional[List[Tuple[float, float, float]]] = None,
) -> Dict[str, float]:
    ensure_fsv_initialized(inst)
    edge_mhz = edge_hz / 1e6
    if abs(edge_mhz - 2390.0) <= 2.0:
        start_hz, stop_hz = 2310e6, 2430e6
    elif abs(edge_mhz - 2483.5) <= 2.0:
        start_hz, stop_hz = 2450e6, 2500e6
    elif abs(edge_mhz - 5150.0) <= 5.0:
        start_hz, stop_hz = 5000e6, 5200e6
    elif abs(edge_mhz - 5350.0) <= 5.0:
        start_hz, stop_hz = 5300e6, 5460e6
    else:
        # Fallback for unknown edge definitions.
        start_hz = edge_hz - span_hz / 2.0
        stop_hz = edge_hz + span_hz / 2.0
    print(
        f"[INFO] Bandedge search window: {start_hz/1e6:.1f}~{stop_hz/1e6:.1f} MHz "
        f"(edge={edge_hz/1e6:.1f} MHz, side={side})"
    )

    inst.send_cmd("*CLS")
    inst.send_cmd("SYST:DISP:UPD ON")
    inst.send_cmd("SENS:FREQ:MODE SWE")
    inst.send_cmd("INIT:CONT OFF")
    inst.send_cmd(f"SENS:FREQ:STAR {start_hz:.0f}")
    inst.send_cmd(f"SENS:FREQ:STOP {stop_hz:.0f}")
    inst.send_cmd("SENS:BAND:AUTO OFF")
    inst.send_cmd("SENS:BAND 1MHz")
    inst.send_cmd("SENS:BAND:VID:AUTO OFF")
    inst.send_cmd("SENS:BAND:VID 1MHz")
    inst.send_cmd("DISP:WIND:SUBW:TRAC1:MODE AVER")
    inst.send_cmd("SENS:WIND:DET1:FUNC RMS")
    inst.send_cmd("SENS:AVER:TYPE POW")
    inst.send_cmd("SENS:AVER:COUN 100")
    inst.send_cmd(f"DISP:WIND:TRAC:Y:SCAL:RLEV {ref_level_dbm}")
    if loss_table:
        loss_db = _lookup_cable_loss_db(edge_hz, loss_table)
        if loss_db is not None:
            inst.send_cmd(f"DISP:WIND:TRAC:Y:SCAL:RLEV:OFFS {loss_db}")
    inst.send_cmd("INP:ATT:AUTO OFF")
    inst.send_cmd("INP:ATT 20")
    inst.send_cmd("CALC1:MARK1:STAT ON")
    inst.send_cmd("CALC1:MARK1:X:SLIM:STAT ON")
    inst.send_cmd(f"CALC1:MARK1:X:SLIM:LEFT {start_hz:.0f}")
    inst.send_cmd(f"CALC1:MARK1:X:SLIM:RIGHT {stop_hz:.0f}")
    if side.upper() == "LEFT":
        inst.send_cmd(f"CALC1:MARK1:X:SLIM:RIGHT {edge_hz:.0f}")
    else:
        inst.send_cmd(f"CALC1:MARK1:X:SLIM:LEFT {edge_hz:.0f}")
    inst.send_cmd("INIT:IMM;*WAI")
    inst.send_cmd("CALC1:MARK1:MAX:PEAK")
    freq_hz = inst.query_float("CALC1:MARK1:X?")
    power_dbm = inst.query_float("CALC1:MARK1:Y?")
    inst.send_cmd("CALC1:MARK1:X:SLIM:STAT OFF")
    return {"freq_hz": freq_hz, "power_dbm": power_dbm}
