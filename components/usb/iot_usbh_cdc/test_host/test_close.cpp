/*
 * SPDX-FileCopyrightText: 2026 OJZen
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <future>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using esp_err_t = int;
using usb_device_handle_t = void*;
using usbh_cdc_port_handle_t = void*;
constexpr int ESP_OK = 0, ESP_ERR_INVALID_ARG = 1, ESP_ERR_INVALID_STATE = 2;
constexpr int portMAX_DELAY = -1;
constexpr int pdTRUE = 1;
#define pdMS_TO_TICKS(ms) (ms)
#define ESP_RETURN_ON_FALSE(ok, err, ...) do { if (!(ok)) return (err); } while (0)
#define ESP_RETURN_ON_ERROR(call, ...) do { int err = (call); if (err) return err; } while (0)
#define ESP_ERROR_CHECK(call) assert((call) == ESP_OK)
#define ESP_ERROR_CHECK_WITHOUT_ABORT(call) ((void)(call))

struct Semaphore {
    std::mutex mutex;
    std::condition_variable changed;
    bool available = true, deleted = false;
    int attempts = 0;
    void waitForAttempts(int count)
    {
        std::unique_lock lock(mutex);
        assert(changed.wait_for(lock, 3s, [&] { return attempts >= count; }));
    }
};
static int xSemaphoreTake(Semaphore* sem, int timeout)
{
    std::unique_lock lock(sem->mutex);
    ++sem->attempts;
    sem->changed.notify_all();
    if (timeout == 0 && !sem->available) {
        return 0;
    }
    assert(sem->changed.wait_for(lock, 3s, [&] { return sem->available; }));
    assert(!sem->deleted);
    sem->available = false;
    return 1;
}
static void xSemaphoreGive(Semaphore* sem)
{
    std::lock_guard lock(sem->mutex);
    assert(!sem->deleted && !sem->available);
    sem->available = true;
    sem->changed.notify_all();
}
static void vSemaphoreDelete(Semaphore* sem)
{
    assert(!sem->deleted);
    sem->deleted = true;
}
static std::mutex pauseMutex;
static std::condition_variable pauseChanged;
static bool pauseNextClose, paused, resumeClose;
static void vTaskDelay(int)
{
    std::unique_lock lock(pauseMutex);
    if (pauseNextClose) {
        pauseNextClose = false;
        paused = true;
        pauseChanged.notify_all();
        assert(pauseChanged.wait_for(lock, 3s, [] { return resumeClose; }));
    }
}
static std::mutex listMutex;
#define CDC_ENTER_CRITICAL() listMutex.lock()
#define CDC_EXIT_CRITICAL() listMutex.unlock()

template<class T> struct List {
    T* first = nullptr;
};
template<class T> struct Link {
    T* next = nullptr;
};
#define SLIST_FOREACH(var, list, field) for ((var) = (list)->first; (var); (var) = (var)->field.next)
#define SLIST_REMOVE(list, item, type, field) do { \
    auto** cursor = &(list)->first; \
    while (*cursor && *cursor != (item)) cursor = &(*cursor)->field.next; \
    assert(*cursor == (item)); \
    *cursor = (*cursor)->field.next; \
} while (0)

struct usb_transfer_t {
    int bEndpointAddress;
};
struct Interface {
    int bInterfaceNumber;
};
struct usbh_cdc_dev_t {
    usb_device_handle_t dev_hdl;
    bool disconnected;
    Link<usbh_cdc_dev_t> list_entry;
};
struct usbh_cdc_port_t {
    usbh_cdc_dev_t* cdc_dev;
    bool to_close;
    struct {
        Semaphore *out_mux, *out_xfer_free_sem;
        usb_transfer_t *in_xfer, *out_xfer;
        const Interface* intf_desc;
    } data;
    struct {
        usb_transfer_t* xfer;
        const Interface* intf_desc;
    } notif;
    struct {
        void *notif_cb, *recv_data, *user_data;
        void (*closed)(usbh_cdc_port_handle_t, void*);
    } cbs;
    void *in_ringbuf_handle, *out_ringbuf_handle;
    Link<usbh_cdc_port_t> list_entry;
};
struct Driver {
    List<usbh_cdc_dev_t> cdc_devices_list;
    List<usbh_cdc_port_t> cdc_devices_port_list;
    Semaphore* open_close_mutex;
    void* cdc_client_hdl;
};
static Driver* p_usbh_cdc_obj;
static int halts, flushes, clears, releases, deviceCloses, transferFrees, closedCallbacks;
static int haltResult, flushResult, clearResult;
static std::set<void*> freed;
static esp_err_t usb_host_endpoint_halt(void*, int)
{
    ++halts;
    return haltResult;
}
static esp_err_t usb_host_endpoint_flush(void*, int)
{
    ++flushes;
    return flushResult;
}
static esp_err_t usb_host_endpoint_clear(void*, int)
{
    ++clears;
    return clearResult;
}
static esp_err_t usb_host_interface_release(void*, void*, int)
{
    ++releases;
    return ESP_OK;
}
static esp_err_t usb_host_device_close(void*, void*)
{
    ++deviceCloses;
    return ESP_OK;
}
static void _cdc_transfers_free(usbh_cdc_port_t* port)
{
    transferFrees += !!port->data.in_xfer + !!port->data.out_xfer + !!port->notif.xfer;
}
static void _cdc_dev_ctrl_free(usbh_cdc_dev_t*) {}
static void _ringbuf_flush(void*) {}
static void vRingbufferDelete(void*) {}
static int get_cdc_port_number_on_device(usbh_cdc_dev_t* device)
{
    int count = 0;
    std::lock_guard lock(listMutex);
    usbh_cdc_port_t* port;
    SLIST_FOREACH(port, &p_usbh_cdc_obj->cdc_devices_port_list, list_entry) {
        count += port->cdc_dev == device;
    }
    return count;
}
// Retain storage until threads join so duplicate frees fail deterministically.
static void checkedFree(void* pointer)
{
    assert(freed.insert(pointer).second);
}
#define free checkedFree
#include "cdc_close_under_test.inc"
#undef free

struct Fixture {
    Driver driver{};
    Semaphore closeMutex;
    std::vector<usbh_cdc_port_t*> ports;
    std::vector<usbh_cdc_dev_t*> devices;
    usb_transfer_t in{1}, out{2}, notify{3};
    Interface dataInterface{1}, notificationInterface{0};
    Fixture()
    {
        p_usbh_cdc_obj = &driver;
        driver.open_close_mutex = &closeMutex;
        halts = flushes = clears = releases = deviceCloses = transferFrees = closedCallbacks = 0;
        haltResult = flushResult = clearResult = 0;
        freed.clear();
    }
    usbh_cdc_dev_t* device()
    {
        auto* value = new usbh_cdc_dev_t{};
        value->dev_hdl = value;
        value->list_entry.next = driver.cdc_devices_list.first;
        driver.cdc_devices_list.first = value;
        devices.push_back(value);
        return value;
    }
    usbh_cdc_port_t* port(usbh_cdc_dev_t* device)
    {
        auto* value = new usbh_cdc_port_t{};
        value->cdc_dev = device;
        value->data.out_mux = new Semaphore;
        value->data.out_xfer_free_sem = new Semaphore;
        value->data.in_xfer = &in;
        value->data.out_xfer = &out;
        value->data.intf_desc = &dataInterface;
        value->notif.xfer = &notify;
        value->notif.intf_desc = &notificationInterface;
        value->cbs.closed = [](void*, void*) {
            ++closedCallbacks;
        };
        value->list_entry.next = driver.cdc_devices_port_list.first;
        driver.cdc_devices_port_list.first = value;
        ports.push_back(value);
        return value;
    }
    ~Fixture()
    {
        for (auto * port : ports) {
            delete port->data.out_mux;
            delete port->data.out_xfer_free_sem;
            delete port;
        }
        for (auto * device : devices) {
            delete device;
        }
        p_usbh_cdc_obj = nullptr;
    }
    void assertClosed(int count)
    {
        assert(!driver.cdc_devices_port_list.first && !driver.cdc_devices_list.first);
        assert(closedCallbacks == count && transferFrees == count * 3);
        assert(halts == count * 3 && flushes == count * 3 && clears == 0);
        assert(releases == count * 2);
        assert(deviceCloses == 1 && freed.size() == static_cast<size_t>(count + 1));
    }
};

static void concurrentClose(bool disconnectFirst)
{
    Fixture fixture;
    auto* device = fixture.device();
    auto* ecm = fixture.port(device);
    auto* at = fixture.port(device);
    device->disconnected = true;
    if (disconnectFirst) {
        std::lock_guard lock(pauseMutex);
        pauseNextClose = true;
        paused = resumeClose = false;
    } else {
        xSemaphoreTake(&fixture.closeMutex, portMAX_DELAY);
    }
    auto owner = std::async(std::launch::async, [&] {
        if (disconnectFirst) assert(!_cdc_close_disconnected_ports());
        else {
            assert(usbh_cdc_port_close(at) == ESP_OK);
        }
    });
    if (disconnectFirst) {
        std::unique_lock lock(pauseMutex);
        assert(pauseChanged.wait_for(lock, 3s, [] { return paused; }));
    } else {
        fixture.closeMutex.waitForAttempts(2); // AT is claimed but cannot be freed yet
    }
    auto duplicate = std::async(std::launch::async, [&] { return usbh_cdc_port_close(at); });
    assert(duplicate.wait_for(1s) == std::future_status::ready);
    assert(duplicate.get() == ESP_ERR_INVALID_ARG);
    if (disconnectFirst) {
        // Claim ECM while automatic cleanup is busy with AT; no cached next pointer is safe.
        auto other = std::async(std::launch::async, [&] { assert(usbh_cdc_port_close(ecm) == ESP_OK); });
        fixture.closeMutex.waitForAttempts(2);
        {
            std::lock_guard lock(pauseMutex);
            resumeClose = true;
            pauseChanged.notify_all();
        }
        owner.get();
        other.get();
    } else {
        // The USB event loop must return immediately while another closer holds its mutex.
        auto cleanup = std::async(std::launch::async, [] { return _cdc_close_disconnected_ports(); });
        assert(cleanup.wait_for(1s) == std::future_status::ready && cleanup.get());
        xSemaphoreGive(&fixture.closeMutex);
        owner.get();
        assert(!_cdc_close_disconnected_ports());
    }
    fixture.assertClosed(2);
    assert(usbh_cdc_port_close(at) == ESP_ERR_INVALID_ARG);
}

int main()
{
    assert(usbh_cdc_port_close(nullptr) == ESP_ERR_INVALID_STATE);
    {
        Fixture fixture;
        assert(usbh_cdc_port_close(nullptr) == ESP_ERR_INVALID_ARG);
        auto* firstDevice = fixture.device();
        auto* first = fixture.port(firstDevice);
        auto* secondDevice = fixture.device();
        auto* second = fixture.port(secondDevice);
        firstDevice->disconnected = true;
        assert(!_cdc_close_disconnected_ports());
        assert(freed.count(first) == 1 && freed.count(second) == 0);
        assert(usbh_cdc_port_close(second) == ESP_OK);
        assert(deviceCloses == 2 && closedCallbacks == 2 && clears == 0);
    }
    for (int iteration = 0; iteration < 20; ++iteration) {
        concurrentClose(false);
        concurrentClose(true);
    }
    {
        Fixture fixture;
        auto* device = fixture.device();
        // Recovery still clears endpoints; closing must not use this path.
        assert(_cdc_reset_transfer_endpoint(device->dev_hdl, &fixture.in) == ESP_OK);
        assert(halts == 1 && flushes == 1 && clears == 1);
        haltResult = ESP_ERR_INVALID_STATE;
        assert(_cdc_reset_transfer_endpoint(device->dev_hdl, &fixture.in) == haltResult);
        assert(flushes == 1 && clears == 1);
        haltResult = 0;
        flushResult = ESP_ERR_INVALID_STATE;
        assert(_cdc_reset_transfer_endpoint(device->dev_hdl, &fixture.in) == flushResult);
        assert(clears == 1);
        flushResult = 0;
        clearResult = ESP_ERR_INVALID_STATE;
        assert(_cdc_reset_transfer_endpoint(device->dev_hdl, &fixture.in) == clearResult);
    }
}
