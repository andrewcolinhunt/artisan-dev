"""Run one tiny CUDA driver-API kernel without a toolkit or framework install."""

from __future__ import annotations

import ctypes
import json
import os
import sys

PTX = b"""
.version 6.4
.target sm_52
.address_size 64
.visible .entry increment(.param .u64 address) {
    .reg .b64 pointer;
    .reg .b32 value;
    ld.param.u64 pointer, [address];
    ld.global.u32 value, [pointer];
    add.u32 value, value, 1;
    st.global.u32 [pointer], value;
    ret;
}
"""


def run_kernel() -> dict[str, object]:
    """Allocate four bytes, increment 41 to 42 on the GPU, and free all resources."""
    driver = ctypes.CDLL("libcuda.so.1")

    def call(name: str, *arguments: object) -> None:
        code = getattr(driver, name)(*arguments)
        if code != 0:
            message = ctypes.c_char_p()
            driver.cuGetErrorName(code, ctypes.byref(message))
            msg = f"{name} failed: {message.value!r} ({code})"
            raise RuntimeError(msg)

    call("cuInit", 0)
    count, device = ctypes.c_int(), ctypes.c_int()
    call("cuDeviceGetCount", ctypes.byref(count))
    assert count.value == 1
    call("cuDeviceGet", ctypes.byref(device), 0)
    name = ctypes.create_string_buffer(128)
    call("cuDeviceGetName", name, len(name), device)
    version = ctypes.c_int()
    call("cuDriverGetVersion", ctypes.byref(version))
    context, module, function = ctypes.c_void_p(), ctypes.c_void_p(), ctypes.c_void_p()
    address = ctypes.c_uint64()
    call("cuCtxCreate_v2", ctypes.byref(context), 0, device)
    try:
        call("cuModuleLoadData", ctypes.byref(module), ctypes.c_char_p(PTX))
        call("cuModuleGetFunction", ctypes.byref(function), module, b"increment")
        call("cuMemAlloc_v2", ctypes.byref(address), ctypes.c_size_t(4))
        try:
            value = ctypes.c_int(41)
            call("cuMemcpyHtoD_v2", address, ctypes.byref(value), ctypes.c_size_t(4))
            parameters = (ctypes.c_void_p * 1)(
                ctypes.cast(ctypes.byref(address), ctypes.c_void_p)
            )
            call(
                "cuLaunchKernel", function, 1, 1, 1, 1, 1, 1, 0, None, parameters, None
            )
            call("cuCtxSynchronize")
            call("cuMemcpyDtoH_v2", ctypes.byref(value), address, ctypes.c_size_t(4))
            assert value.value == 42
        finally:
            call("cuMemFree_v2", address)
        call("cuModuleUnload", module)
    finally:
        call("cuCtxDestroy_v2", context)
    return {
        "status": "passed",
        "input": 41,
        "output": 42,
        "device_count": count.value,
        "device": name.value.decode(),
        "driver_api_version": version.value,
        "executable": sys.executable,
        "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
    }


if __name__ == "__main__":
    sys.stdout.write(json.dumps(run_kernel(), indent=2) + "\n")
