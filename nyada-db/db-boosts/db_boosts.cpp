// dict_serializer.cpp
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <Python.h>
#include <stdexcept>
#include <string_view>
#include <cstdint>
#include <cstring>

namespace py = pybind11;

// write varint
static inline size_t encode_varint_buf(char *o, uint64_t v) {
	size_t c = 0;
	while (v >= 0x80) {
		o[c++] = char((v & 0x7F) | 0x80);
		v >>= 7;
	}
	o[c++] = char(v);
	return c;
}

// read varint
static inline uint64_t decode_varint_buf(const char *o, size_t &p, size_t e) {
	uint64_t r = 0;
	int s = 0;
	while (p < e) {
		uint8_t b = o[p++];
		r |= uint64_t(b & 0x7F) << s;
		if (!(b & 0x80)) break;
		s += 7;
	}
	return r;
}

// serialize dict->bytes
static py::bytes serialize_dict(const py::dict &d) {
	size_t n = PyDict_Size(d.ptr());

	// calc size
	size_t total = 0, tmp = n;
	do { total++; tmp >>= 7; } while (tmp);

	PyObject *k, *v;
	Py_ssize_t p = 0;
	while (PyDict_Next(d.ptr(), &p, &k, &v)) {
		size_t kl = PyBytes_Size(k), vl = PyBytes_Size(v);
		tmp = kl; do { total++; tmp >>= 7; } while (tmp);
		total += kl;
		tmp = vl; do { total++; tmp >>= 7; } while (tmp);
		total += vl;
	}

	// alloc bytes
	PyObject *bobj = PyBytes_FromStringAndSize(nullptr, (Py_ssize_t)total);
	if (!bobj) throw std::runtime_error("alloc");
	char *buf = PyBytes_AsString(bobj);

	// fill
	size_t pos = 0;
	pos += encode_varint_buf(buf + pos, n);
	p = 0;
	while (PyDict_Next(d.ptr(), &p, &k, &v)) {
		size_t kl = PyBytes_Size(k), vl = PyBytes_Size(v);
		const char *kp = PyBytes_AsString(k), *vp = PyBytes_AsString(v);
		pos += encode_varint_buf(buf + pos, kl);
		memcpy(buf + pos, kp, kl); pos += kl;
		pos += encode_varint_buf(buf + pos, vl);
		memcpy(buf + pos, vp, vl); pos += vl;
	}

	return py::reinterpret_steal<py::bytes>(bobj);
}

// deserialize bytes->dict
static py::dict deserialize_dict(const py::bytes &b) {
	char *dptr; Py_ssize_t dlen;
	PyBytes_AsStringAndSize(b.ptr(), &dptr, &dlen);

	size_t pos = 0;
	uint64_t n = decode_varint_buf(dptr, pos, (size_t)dlen);
	py::dict r;

	for (uint64_t i = 0; i < n; ++i) {
		uint64_t kl = decode_varint_buf(dptr, pos, (size_t)dlen);
		if (pos + kl > (size_t)dlen) throw std::runtime_error("trunc key");
		py::bytes key(dptr + pos, (Py_ssize_t)kl); pos += kl;

		uint64_t vl = decode_varint_buf(dptr, pos, (size_t)dlen);
		if (pos + vl > (size_t)dlen) throw std::runtime_error("trunc val");
		py::bytes val(dptr + pos, (Py_ssize_t)vl); pos += vl;

		r[key] = val;
	}

	return r;
}

PYBIND11_MODULE(db_boosts, m) {
	m.doc() = "fast dict serializer";
	m.def("serialize", &serialize_dict);
	m.def("deserialize", &deserialize_dict);
}
