from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from oc4ids_datastore_pipeline.notifications import send_notification


def test_send_notification(mocker: MockerFixture) -> None:
    mock_smtp_server = MagicMock()
    patch_smtp = mocker.patch("oc4ids_datastore_pipeline.notifications.smtplib.SMTP")
    patch_smtp.return_value = mock_smtp_server

    errors = [
        {
            "dataset_id": "test_dataset",
            "source_url": "https://test_dataset.json",
            "message": "Mocked exception",
        }
    ]
    send_notification(errors)

    patch_smtp.assert_called_once_with("localhost", 8025)
    with mock_smtp_server as server:
        server.sendmail.assert_called_once()
        sender, receiver, message = server.sendmail.call_args[0]
        assert sender == "sender@example.com"
        assert receiver == "receiver@example.com"
        assert "test_dataset" in message
        assert "https://test_dataset.json" in message
        assert "Mocked exception" in message
