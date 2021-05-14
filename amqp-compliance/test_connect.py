import pika
import pytest

def connect_to_default_vhost(username, password):
    return pika.BlockingConnection(
            pika.ConnectionParameters(
                host='localhost',
                port=5672,
                virtual_host='/',
                credentials=pika.PlainCredentials(username, password)
                )
            )

def test_connect_success():
    """
    Connecting with correct user credentials ends up in success, no exceptions are thrown.
    """
    connect_to_default_vhost('guest', 'guest')

def test_connect_fail_bad_password():
    """
    Connecting with incorrect credentials ends up in AuthenticationError.
    """
    with pytest.raises(pika.exceptions.ProbableAuthenticationError) as exp:
        connect_to_default_vhost('guest', 'pwd')

    assert str(exp.value).startswith('ConnectionClosedByBroker: (403) \'ACCESS_REFUSED')
