import helper
import pika
import pytest

def test_connect_success():
    """
    Connecting with correct user credentials ends up in success, no exceptions are thrown.
    """
    helper.connect()

def test_connect_fail_bad_password():
    """
    Connecting with incorrect credentials ends up in AuthenticationError.
    """
    with pytest.raises(pika.exceptions.ProbableAuthenticationError) as exp:
        helper.connect(password='pwd')

    assert str(exp.value).startswith('ConnectionClosedByBroker: (403) \'ACCESS_REFUSED')
