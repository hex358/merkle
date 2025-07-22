#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <Python.h>
#include <pybind11/buffer_info.h>
#include <vector>
#include <iostream>
#include <string>
#include <chrono>
namespace py = pybind11;
using namespace std;

static inline size_t varint_size(uint64_t v) {
	size_t sz = 0;
	do { sz++; v >>= 7; } while (v);
	return sz;
}

static inline size_t encode_varint(uint64_t v, char* out) {
	size_t i = 0;
	while (v >= 0x80) {
		out[i++] = static_cast<char>((v & 0x7F) | 0x80);
		v >>= 7;
	}
	out[i++] = static_cast<char>(v);
	return i;
}

static inline void decode_varint(const char*& ptr, const char* end, uint64_t& v) {
	v = 0;
	int shift = 0;
	while (ptr < end) {
		uint8_t byte = static_cast<uint8_t>(*ptr++);
		v |= uint64_t(byte & 0x7F) << shift;
		if (!(byte & 0x80)) break;
		shift += 7;
	}
}

// fixed‑length helpers
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



// varint-based
py::bytes serialize_varint(py::dict dict) {
	Py_ssize_t pos{};
	PyObject *key, *value;
	uint64_t count{};
	size_t total{};

	struct IT { char* k; size_t kl; char* v; size_t vl; size_t ksz; size_t vsz; };
	vector<IT> items;
	items.reserve(PyDict_Size(dict.ptr()));

	// collect and size
	while (PyDict_Next(dict.ptr(), &pos, &key, &value)) {
		char *kp, *vp; Py_ssize_t kl, vl;
		if (PyBytes_AsStringAndSize(key, &kp, &kl) < 0 ||
		    PyBytes_AsStringAndSize(value, &vp, &vl) < 0)
			throw runtime_error("Only dict[bytes,bytes] supported");
		size_t ksz = varint_size(kl);
		size_t vsz = varint_size(vl);
		items.push_back({kp, size_t(kl), vp, size_t(vl), ksz, vsz});
		total += ksz + kl + vsz + vl;
		++count;
	}
	total += varint_size(count);

	// allocate output buffer
	PyObject* out_py = PyBytes_FromStringAndSize(nullptr, (Py_ssize_t)total);
	char* out = PyBytes_AS_STRING(out_py);
	char* p = out;

	// write count
	p += encode_varint(count, p);

	// drop GIL for heavy writes
	{
		py::gil_scoped_release release;
		for (auto &it : items) {
			p += encode_varint(it.kl, p);
			memcpy(p, it.k, it.kl);
			p += it.kl;
			p += encode_varint(it.vl, p);
			memcpy(p, it.v, it.vl);
			p += it.vl;
		}
	}
	return py::reinterpret_steal<py::bytes>(py::handle(out_py));
}

// fixed-length (max speed)
py::bytes serialize_fast32(py::dict dict) {
	Py_ssize_t pos{};
	PyObject *key, *value;
	uint64_t count{};
	size_t total = 8;  // 8 bytes for entry count

	struct IT { char* k; size_t kl; char* v; size_t vl; };
	vector<IT> items;
	items.reserve(PyDict_Size(dict.ptr()));

	// collect and size
	while (PyDict_Next(dict.ptr(), &pos, &key, &value)) {
		char *kp, *vp; Py_ssize_t kl, vl;
		if (PyBytes_AsStringAndSize(key, &kp, &kl) < 0 ||
		    PyBytes_AsStringAndSize(value, &vp, &vl) < 0)
			throw runtime_error("Only dict[bytes,bytes] supported");
		items.push_back({kp, size_t(kl), vp, size_t(vl)});
		total += 4 + kl + 4 + vl;
		++count;
	}

	// allocate
	PyObject* out_py = PyBytes_FromStringAndSize(nullptr, (Py_ssize_t)total);
	char* out = PyBytes_AS_STRING(out_py);
	char* p = out;

	// write entry count
	write_u64_le(count, p);
	p += 8;

	// drop GIL for bulk writes
	{
		py::gil_scoped_release release;
		for (auto &it : items) {
			write_u32_le(uint32_t(it.kl), p);
			p += 4;
			memcpy(p, it.k, it.kl);
			p += it.kl;
			write_u32_le(uint32_t(it.vl), p);
			p += 4;
			memcpy(p, it.v, it.vl);
			p += it.vl;
		}
	}
	return py::reinterpret_steal<py::bytes>(py::handle(out_py));
}

// deserializer
py::dict deserialize_varint(py::bytes blob) {
	py::buffer_info info(py::buffer(blob).request());
	const char* ptr = static_cast<const char*>(info.ptr);
	const char* end = ptr + info.size;
	uint64_t count;
	decode_varint(ptr, end, count);

	py::dict result;
	for (uint64_t i = 0; i < count; ++i) {
		uint64_t kl, vl;
		decode_varint(ptr, end, kl);
		py::bytes key(ptr, (py::ssize_t)kl);
		ptr += kl;
		decode_varint(ptr, end, vl);
		py::bytes val(ptr, (py::ssize_t)vl);
		ptr += vl;
	    result[key] = val;
	}
	return result;
}


py::dict deserialize_fast32(py::bytes blob) {
	// raw buffer
	auto info = py::buffer(blob).request();
	auto ptr  = static_cast<const uint8_t*>(info.ptr);

	// считываем count
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

PYBIND11_MODULE(db_boosts, m) {
	m.doc() = "c++ boosts for db";
	m.def("serialize_varint", &serialize_varint, "Compact varint mode");
	m.def("serialize_fast32", &serialize_fast32, "Fixed‑length mode (4B lengths + 8B count)");
	m.def("deserialize_varint", &deserialize_varint, "Deserialize varint blobs");
	m.def("deserialize_fast32", &deserialize_fast32, "Deserialize fixed-length blobs");
}
