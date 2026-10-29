# CDC close regression test

This standalone C++17 test compiles the close and endpoint helpers extracted
from `../iot_usbh_cdc.c`. FreeRTOS synchronization, USB endpoints and resource
release are substituted with host implementations; no ESP-IDF or USB device
is required.

Run from the repository root with CMake and a C++17 compiler:

```sh
cmake -S components/usb/iot_usbh_cdc/test_host -B build/cdc-close -DCMAKE_BUILD_TYPE=Debug
cmake --build build/cdc-close --config Debug
ctest --test-dir build/cdc-close -C Debug --output-on-failure
```

The test uses barriers to force both orderings of application close versus
disconnect cleanup, including two ports sharing a device. It checks that a
second closer is rejected before waiting on the close mutex, each resource is
released once, a busy close mutex does not block disconnect cleanup, and only
the disconnected device is cleaned up. It also verifies that close halts and
flushes endpoints without clearing them, while error recovery still clears
endpoints and propagates errors.

The host test does not model USB enumeration, in-flight URB completion or real
FreeRTOS scheduling. The existing ESP-IDF test app and hardware hot-plug tests
are still required to validate those paths.
