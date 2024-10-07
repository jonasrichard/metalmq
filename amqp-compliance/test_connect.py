import pika
import pika.exceptions
import pytest

import helper

def test_connect_success():
    """
    Connecting with correct user credentials ends up in success, no exceptions are
    thrown.
    """
    conn = helper.connect()
    conn.close()

def test_connect_fail_bad_password():
    """
    Connecting with incorrect credentials ends up in AuthenticationError.
    """
    with pytest.raises(pika.exceptions.ProbableAuthenticationError) as exp:
        helper.connect(password="pwd")

def test_reopen_the_same_channel():
    """
    Open the same channel results in a channel error.
    """
    with helper.connection() as conn:
        conn.channel(17)

        with pytest.raises(pika.exceptions.ConnectionClosedByBroker) as exp:
            conn.channel(17)

        assert 504 == exp.value.reply_code
        assert str(exp.value.reply_text).startswith("CHANNEL_ERROR")
