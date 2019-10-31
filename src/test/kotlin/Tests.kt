import com.ericsson.otp.erlang.OtpErlangAtom
import com.ericsson.otp.erlang.OtpErlangObject
import com.ericsson.otp.erlang.OtpErlangString
import com.ericsson.otp.erlang.OtpErlangTuple
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class Tests {

    val ok = OtpErlangAtom("ok")

    @Test
    fun stuffTest() {
        val tuple = tuple(ok, OtpErlangString("hello"))

        when (is_ok(tuple)) {
            true -> {
                val value = (tuple as OtpErlangTuple).elementAt(1) as OtpErlangString
                println(value)
            }
            else -> println("Suck")
        }
    }

    private fun is_ok(term: OtpErlangObject) = term is OtpErlangTuple && is_atom(term.elementAt(0), "ok")

    private fun is_atom(term: OtpErlangObject, atom_text: String) = when (term) {
        is OtpErlangAtom -> term.atomValue() == atom_text
        else -> false
    }

    private fun tuple(vararg elements: OtpErlangObject) = OtpErlangTuple(elements)
}