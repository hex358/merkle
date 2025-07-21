#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <Python.h>
#include <stdexcept>
#include <string>
#include <string_view>

using namespace std;
namespace py = pybind11;

// encode len as varint
static void encode_varint(string &out, uint64_t v) {
	while (v >= 0x80) {
		out.push_back(char((v & 0x7F) | 0x80));
		v >>= 7;
	}
	out.push_back(char(v));
}

// decode len from varint
static uint64_t decode_varint(string_view view, size_t &pos) {
	uint64_t r = 0;
	int shift = 0;
	while (pos < view.size()) {
		uint8_t b = static_cast<uint8_t>(view[pos++]);
		r |= uint64_t(b & 0x7F) << shift;
		if (!(b & 0x80)) break;
		shift += 7;
	}
	return r;
}

// serialize dict[bytes,bytes] -> bytes
static string serialize_dict(const py::dict &d) {
	string out;
	size_t n = PyDict_Size(d.ptr());
	out.reserve(n * 16);
	encode_varint(out, n);

	PyObject *k, *v;
	Py_ssize_t p = 0;
	while (PyDict_Next(d.ptr(), &p, &k, &v)) {
		if (!PyBytes_Check(k) || !PyBytes_Check(v))
			throw invalid_argument("bytes only");

		Py_ssize_t kl = PyBytes_Size(k);
		const char *kp = PyBytes_AsString(k);
		Py_ssize_t vl = PyBytes_Size(v);
		const char *vp = PyBytes_AsString(v);

		encode_varint(out, uint64_t(kl));
		out.append(kp, kl);
		encode_varint(out, uint64_t(vl));
		out.append(vp, vl);
	}

	return out;
}

// deserialize bytes -> dict[bytes,bytes]
static py::dict deserialize_dict(const py::bytes &b) {
	char *data;
	Py_ssize_t dl;
	PyBytes_AsStringAndSize(b.ptr(), &data, &dl);
	string_view view(data, dl);

	size_t pos = 0;
	uint64_t n = decode_varint(view, pos);
	py::dict res;

	for (uint64_t i = 0; i < n; ++i) {
		uint64_t kl = decode_varint(view, pos);
		if (pos + kl > view.size()) throw runtime_error("trunc key");
		py::bytes k(view.data() + pos, kl);
		pos += kl;

		uint64_t vl = decode_varint(view, pos);
		if (pos + vl > view.size()) throw runtime_error("trunc val");
		py::bytes v(view.data() + pos, vl);
		pos += vl;

		res[k] = v;
	}

	return res;
}

PYBIND11_MODULE(db_boosts, m) {
	m.doc() = "c++ speedups for my database";
	m.def("serialize", &serialize_dict, "serialize");
	m.def("deserialize", &deserialize_dict, "deserialize");
}
