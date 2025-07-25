#include <Python.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>
#include <pybind11/buffer_info.h>
#include <vector>
#include <string>
#include <unordered_map>
#include <cstdint>
#include <cstring>
#include <stdexcept>
namespace py = pybind11;


static inline void write_u32_le(uint32_t x, char* out) {
	out[0] = char(x);
	out[1] = char(x >> 8);
	out[2] = char(x >> 16);
	out[3] = char(x >> 24);
}



static inline void write_u64_le(uint64_t x, char* out) {
	write_u32_le(uint32_t(x), out);
	write_u32_le(uint32_t(x >> 32), out + 4);
}



py::bytes serialize(py::dict dict) {
    Py_ssize_t pos = 0;
    PyObject *key, *value;
    uint64_t count = 0;
    size_t total = 8;

    struct IT { const char* k; size_t kl; const char* v; size_t vl; };
    std::vector<IT> items;
    items.reserve(PyDict_Size(dict.ptr()));

    while (PyDict_Next(dict.ptr(), &pos, &key, &value)) {
        char *kp, *vp;
        Py_ssize_t kl, vl;

        if (PyBytes_AsStringAndSize(key, &kp, &kl) < 0)
            throw std::runtime_error("serialize: non-bytes key");
        if (PyBytes_AsStringAndSize(value, &vp, &vl) < 0)
            throw std::runtime_error("serialize: non-bytes value");

        items.push_back({ kp, size_t(kl), vp, size_t(vl) });
        total += 4 + size_t(kl) + 4 + size_t(vl);
        ++count;
    }

    PyObject* out_py = PyBytes_FromStringAndSize(nullptr, (Py_ssize_t)total);
    char* out = PyBytes_AS_STRING(out_py);
    char* p = out;

    write_u64_le(count, p);
    p += 8;

    {
        py::gil_scoped_release release;
        for (auto &it : items) {
            write_u32_le(uint32_t(it.kl), p);    p += 4;
            memcpy(p, it.k, it.kl);              p += it.kl;
            write_u32_le(uint32_t(it.vl), p);    p += 4;
            memcpy(p, it.v, it.vl);              p += it.vl;
        }
    }
    return py::reinterpret_steal<py::bytes>(py::handle(out_py));
}



py::dict deserialize(py::bytes blob) {
	// raw buffer
	auto info = py::buffer(blob).request();
	auto ptr  = static_cast<const uint8_t*>(info.ptr);

	uint64_t count =
		  uint64_t(*reinterpret_cast<const uint32_t*>(ptr))
		| (uint64_t(*reinterpret_cast<const uint32_t*>(ptr + 4)) << 32);
	ptr += 8;

    // pre-initialize
	PyObject* raw = _PyDict_NewPresized(static_cast<Py_ssize_t>(count));
	py::dict result = py::reinterpret_steal<py::dict>(py::handle(raw));

	for (uint64_t i = 0; i < count; ++i) {
		// key
		uint32_t kl = *reinterpret_cast<const uint32_t*>(ptr);
		ptr += 4;
		PyObject* key = PyBytes_FromStringAndSize(nullptr, kl);
		memcpy(PyBytes_AS_STRING(key), ptr, kl);
		ptr += kl;
		// compute hash
		Py_hash_t h = PyObject_Hash(key);

		// val
		uint32_t vl = *reinterpret_cast<const uint32_t*>(ptr);
		ptr += 4;
		PyObject* val = PyBytes_FromStringAndSize(nullptr, vl);
		memcpy(PyBytes_AS_STRING(val), ptr, vl);
		ptr += vl;

		// no re-hashing
		_PyDict_SetItem_KnownHash(raw, key, val, h);

		Py_DECREF(key);
		Py_DECREF(val);
	}

	return result;
}


py::bytes bucket(py::bytes bytes_obj, size_t num_buckets) {
	auto info = py::buffer(bytes_obj).request();
	const uint8_t* data = static_cast<const uint8_t*>(info.ptr);
    size_t len = info.size;

	// djb2 initialization
	uint64_t h = 5381;
	// compute djb2 hash: h = h * 33 + c
	for (size_t i = 0; i < len; ++i) {
		h = (h << 5) + h + data[i];
	}

	// bucket index
	uint64_t idx = h % num_buckets;

	// little-endian
	char out[8];
	for (int i = 0; i < 8; ++i) {
		out[i] = static_cast<char>((idx >> (i * 8)) & 0xFF);
	}

	return py::bytes(out, 8);
}

py::bytes pack_index(uint64_t idx) {
    return py::bytes(reinterpret_cast<const char*>(&idx),
                     sizeof(idx));};

static py::bytearray patch_constant_length(const py::buffer &blob_buf,
                                           const py::dict &assigns,
                                           size_t bl)
{
    auto info = blob_buf.request();
    if (info.ndim != 1)
        throw std::runtime_error("patch_constant_length: only 1D buffers supported");
    size_t total = info.size;
    const char *src = static_cast<const char*>(info.ptr);

    PyObject *ba = PyByteArray_FromStringAndSize(nullptr, total);
    if (!ba) throw std::bad_alloc();
    char *dst = PyByteArray_AS_STRING(reinterpret_cast<PyByteArrayObject*>(ba));
    std::memcpy(dst, src, total);

    for (auto it = assigns.begin(); it != assigns.end(); ++it) {
        py::handle key_h = it->first;
        py::handle val_h = it->second;

        size_t slot = py::cast<size_t>(key_h);

        py::object vobj = py::reinterpret_borrow<py::object>(val_h);
        py::buffer  vbuf(vobj);
        auto vinfo = vbuf.request();

        if (vinfo.ndim != 1 || (size_t)vinfo.size != bl)
            throw std::runtime_error(
                "patch_constant_length: slot " + std::to_string(slot) +
                " must be exactly " + std::to_string(bl) + " bytes"
            );

        size_t start = slot * bl;
        if (start + bl > total)
            throw std::out_of_range("write past end of blob");

        std::memcpy(dst + start, vinfo.ptr, bl);
    }

    return py::reinterpret_steal<py::bytearray>(py::handle(ba));
}

using namespace pybind11::literals;
PYBIND11_MODULE(db_boosts, m) {
	m.doc() = "c++ boosts for db";
	m.def("serialize", &serialize, "Fixed‑length serialization (4B lengths + 8B count)");
	m.def("deserialize", &deserialize, "Deserialize fixed-length blobs");
	m.def("bucket", &bucket, "DJB2 bucketing implementation");
    m.def("patch_constant_length",
    &patch_constant_length,
    "Overwrite fixed-size slots in a blob (constant_length branch)");
}
