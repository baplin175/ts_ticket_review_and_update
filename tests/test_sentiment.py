from unittest.mock import patch


def test_sentiment_records_no_when_no_customer_comments():
    import run_sentiment

    with patch("db._is_enabled", return_value=True), \
         patch("db.ticket_ids_for_numbers", return_value={"110016": 21038155}), \
         patch("db.get_current_hashes", return_value={"thread_hash": "abc"}), \
         patch("run_sentiment._should_skip", return_value=False), \
         patch("run_sentiment._load_prompt_record", return_value={"content": "Input:", "version": "test"}), \
         patch("run_sentiment._load_customer_comments_from_db", return_value=[]), \
         patch("run_sentiment._persist_to_db") as persist_mock, \
         patch("run_sentiment.call_matcha") as matcha_mock:
        run_sentiment.main(activities_file=None, force=False, ticket_numbers=["110016"])

    matcha_mock.assert_not_called()
    persist_mock.assert_called_once()
    response_obj = persist_mock.call_args.args[3]
    assert response_obj["frustrated"] == "No"
    assert response_obj["frustrated_reason"] is None
    assert response_obj["ticket_number"] == "110016"
