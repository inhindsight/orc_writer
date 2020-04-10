package orsimer

import com.ericsson.otp.erlang.OtpErlangObject
import com.ericsson.otp.erlang.OtpErlangTuple
import otp.asAtom

class Command(val action: String, val data: List<OtpErlangObject>) {

    companion object {
        fun fromMessage(message: OtpErlangObject): Command {
            val tuple = message as OtpErlangTuple
            val action = tuple.elementAt(0).asAtom()
            val data = tuple.elements().drop(1)

            return Command(action, data)
        }
    }

}