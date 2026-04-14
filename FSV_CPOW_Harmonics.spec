# -*- mode: python ; coding: utf-8 -*-

import os
import shutil
from PyInstaller.config import CONF
from PyInstaller.building.api import COLLECT as _COLLECT


class COLLECT_WITH_INTERNAL(_COLLECT):
    def assemble(self):
        super().assemble()
        dist_dir = os.path.join(CONF["distpath"], self.name)
        internal_dir = os.path.join(dist_dir, "_internal")
        # PyInstaller's default onedir layout keeps dependencies in _internal.
        # Move config/result out to top-level so they sit next to the exe.
        for name in ("config", "result", "result_bandedge", "result_bt"):
            src = os.path.join(internal_dir, name)
            dst = os.path.join(dist_dir, name)
            if os.path.exists(src):
                if os.path.exists(dst):
                    shutil.rmtree(dst, ignore_errors=True)
                shutil.copytree(src, dst, dirs_exist_ok=True)
                shutil.rmtree(src, ignore_errors=True)


a = Analysis(
    ['FSV_CPOW_Harmonics.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('config\\loss.txt', 'config'),
        ('config\\loss_C3cable.txt', 'config'),
        ('config\\loss_Dule_Antenna.txt', 'config'),
        ('config\\FCC_test_item_BT_BLE.xlsx', 'config'),
        ('config\\FCC_test_item_Bandedge.xlsx', 'config'),
        ('config\\FCC_test_item_Dule_Antenna.xlsx', 'config'),
        ('config\\FCC_test_item_dule_band.xlsx', 'config'),
        ('config\\FCC_test_item_single_band.xlsx', 'config'),
        ('result\\.keep', 'result'),
        ('result_bandedge\\.keep', 'result_bandedge'),
        ('result_bt\\.keep', 'result_bt'),
        ('GUI control\\GUI_control.py', 'GUI control'),
    ],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    exclude_binaries=True,
    name='FCC_cond_Autotest',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT_WITH_INTERNAL(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='FCC_cond_Autotest',
)
