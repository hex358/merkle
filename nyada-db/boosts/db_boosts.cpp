#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <Python.h>
#include <pybind11/buffer_info.h>
#include <vector>
namespace py = pybind11;
using namespace std;


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

using namespace pybind11::literals;
PYBIND11_MODULE(db_boosts, m) {
	m.doc() = "c++ boosts for db";
	m.def("serialize", &serialize, "Fixed‑length serialization (4B lengths + 8B count)");
	m.def("deserialize", &deserialize, "Deserialize fixed-length blobs");
	m.def("bucket", &bucket, "DJB2 bucketing implementation");
}
