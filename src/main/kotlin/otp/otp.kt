package otp

import com.ericsson.otp.erlang.*

fun ok() = OtpErlangAtom("ok")

fun string(string: String) = OtpErlangBinary(string.toByteArray())

fun atom(string: String) = OtpErlangAtom(string)

fun long(long: Long) = OtpErlangLong(long)

fun double(value: Double) = OtpErlangDouble(value)

fun boolean(bool: Boolean) = OtpErlangBoolean(bool)

fun tuple(vararg items: OtpErlangObject) = OtpErlangTuple(items)

fun list(vararg items: OtpErlangObject) = OtpErlangList(items)

fun map(vararg pairs: Pair<String, OtpErlangObject>): OtpErlangMap {
    val keys = pairs.map { it.first }.map { string(it) }.toTypedArray()
    val values = pairs.map { it.second }.toTypedArray()

    return OtpErlangMap(keys, values)
}

fun OtpErlangObject.asString(): String = String((this as OtpErlangBinary).binaryValue())

fun OtpErlangObject.asAtom(): String = (this as OtpErlangAtom).atomValue()

fun OtpErlangObject.asMap(): Map<String, Any> {
    val erlangMap = this as OtpErlangMap
    val pairs = erlangMap.keys()
        .zip(erlangMap.values())
        .map { it.first.asString() to it.second }

    return mapOf(*pairs.toTypedArray())
}

