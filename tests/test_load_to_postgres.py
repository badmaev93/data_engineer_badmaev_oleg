import pytest
from unittest.mock import patch, MagicMock

# Import the function to be tested
from dags.forum_data_generation import _load_to_postgres

@patch("dags.forum_data_generation.PostgresHook")
@patch("psycopg2.extras.execute_values")
@patch("dags.forum_data_generation._load")
def test_load_to_postgres_success(mock_load, mock_execute_values, mock_postgres_hook):
    # Setup mock data for each call to _load
    mock_forum_data = {
        "id": "f1", "slug": "forum", "name": "Forum", "description": "Desc", "position": 1
    }
    mock_users_data = [
        {"id": "u1", "username": "user1", "email": "u1@e.com", "display_name": "U1", "status": "active", "joined_at": "2023-01-01"}
    ]
    mock_topics_data = [
        {"id": "t1", "forum_id": "f1", "author_id": "u1", "slug": "topic", "title": "Topic", "status": "open", "created_at": "2023-01-01", "last_post_at": "2023-01-01"}
    ]
    mock_posts_data = [
        {"id": "p1", "topic_id": "t1", "author_id": "u1", "author_guest_name": None, "body": "Post", "created_at": "2023-01-01"}
    ]
    mock_logs_data = [
        {"id": "l1", "user_id": "u1", "session_or_ip": "1.1.1.1", "action_type": "login", "description": "Log", "target_object_id": None, "response_status": "ok", "response_code": 200, "created_at": "2023-01-01"}
    ]

    # _load is called 5 times in this order: forum, users, topics, posts, user_logs
    mock_load.side_effect = [
        mock_forum_data,
        mock_users_data,
        mock_topics_data,
        mock_posts_data,
        mock_logs_data
    ]

    # Setup database mocks
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_postgres_hook.return_value.get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    # Call the function
    _load_to_postgres()

    # Verify connection and cursor setup
    mock_postgres_hook.assert_called_once_with(postgres_conn_id="forum_db")
    mock_postgres_hook.return_value.get_conn.assert_called_once()
    mock_conn.cursor.assert_called_once()

    # Verify TRUNCATE and single INSERT for forums
    expected_calls = [
        ("TRUNCATE user_logs, posts, topics, forums, users CASCADE;",),
        ("INSERT INTO forums (id, slug, name, description, position) VALUES (%s,%s,%s,%s,%s)",
         ("f1", "forum", "Forum", "Desc", 1))
    ]

    # Assert execute was called correctly (twice)
    assert mock_cur.execute.call_count == 2
    actual_calls = [call[0] for call in mock_cur.execute.call_args_list]
    assert actual_calls == expected_calls

    # Verify bulk inserts via execute_values (4 calls: users, topics, posts, user_logs)
    assert mock_execute_values.call_count == 4

    # Verify commit and close
    mock_conn.commit.assert_called_once()
    mock_cur.close.assert_called_once()
    mock_conn.close.assert_called_once()

@patch("dags.forum_data_generation.PostgresHook")
@patch("psycopg2.extras.execute_values")
@patch("dags.forum_data_generation._load")
def test_load_to_postgres_exception(mock_load, mock_execute_values, mock_postgres_hook):
    # Setup mock to simulate an exception during execution
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_postgres_hook.return_value.get_conn.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    mock_forum_data = {
        "id": "f1", "slug": "forum", "name": "Forum", "description": "Desc", "position": 1
    }
    mock_load.side_effect = [
        mock_forum_data,
        [],
        [],
        [],
        []
    ]

    # Simulate an error on the commit
    mock_conn.commit.side_effect = Exception("DB Commit Error")

    # The exception might not be caught, but we need to see what happens to the cursor and connection
    with pytest.raises(Exception, match="DB Commit Error"):
        _load_to_postgres()

    # Verify commit was attempted
    mock_conn.commit.assert_called_once()
    mock_cur.close.assert_called_once()
    mock_conn.close.assert_called_once()
