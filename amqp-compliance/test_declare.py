import helper
import pika
import pytest

class Declare():

    def __init__(
            self,
            exchange_name=None,
            exchange_type="direct",
            passive=False,
            durable=False,
            auto_delete=False,
            callback=None
            ):
        self.connection = helper.connect()
        self.channel = self.connection.channel(channel_number=1)

        self.channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                passive=passive,
                durable=durable,
                auto_delete=auto_delete)

    def close(self, cleanup=False):
        self.channel.close()
        self.connection.close()

def test_passive_declare_non_existent_exchange(caplog):
    with pytest.raises(pika.exceptions.ChannelClosedByBroker) as exp:
        Declare(exchange_name="non-existent", passive=True)

    assert 404 == exp.value.reply_code
    assert str(exp.value.reply_text).startswith("NOT_FOUND - no exchange 'non-existent' in vhost '/'")

@pytest.mark.skip(reason="RabbitMQ allows these characters")
def test_exchange_name_contains_disallowed_char(caplog):
    with pytest.raises(pika.exceptions.ChannelClosedByBroker) as exp:
        Declare(exchange_name="exchange$#'!")

    assert 406 == exp.value.reply_code
    assert str(exp.value.reply_text).startswith("PRECONDITION_FAILED")

def test_exchange_with_not_supported_type(caplog):
    with pytest.raises(pika.exceptions.ChannelClosedByBroker) as exp:
        Declare(exchange_name="topic-not-exist", exchange_type="broadcast")

    assert 406 == exp.value.reply_code
    assert str(exp.value.reply_text).startswith("PRECONDITION_FAILED")

def test_exchange_redeclare_with_different_type(caplog):
    d = Declare(exchange_name="redecl-direct", exchange_type="direct")
    d.close()

    with pytest.raises(pika.exceptions.ChannelClosedByBroker) as exp:
        d2 = Declare(exchange_name="redecl-direct", exchange_type="fanout")

    assert 406 == exp.value.reply_code
    assert str(exp.value.reply_text).startswith("PRECONDITION_FAILED")

@pytest.mark.skip(reason="Auto delete deletes the exchange later")
def test_exchange_auto_delete(caplog):
    d = Declare(exchange_name="xchg-auto-delete", auto_delete=True)
    d.close()

    with pytest.raises(pika.exceptions.ChannelClosedByBroker) as exp:
        Declare(exchange_name="xchg-auto-delete", passive=True)

    assert 406 == exp.value.reply_code
    assert exp.value.reply_text.startswith("PRECONDITION_FAILED")
