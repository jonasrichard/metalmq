import pika
import pytest

import helper

def test_connect_success():
    """
    Connecting with correct user credentials ends up in success, no exceptions are thrown.
    """
    conn = helper.connect()
    conn.close()

def test_connect_fail_bad_password():
    """
    Connecting with incorrect credentials ends up in AuthenticationError.
    """
    with pytest.raises(pika.exceptions.ProbableAuthenticationError) as exp:
        helper.connect(password="pwd")

    assert 403 == exp.value.reply_code
    assert str(exp.value.reply_text).startswith("ACCESS_REFUSED")
