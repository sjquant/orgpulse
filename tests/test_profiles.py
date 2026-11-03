from unittest.mock import Mock

from orgpulse.libs.github import avatar_urls, pull_request_commit_counts


def test_profile_lookup_batches_variables_and_ignores_missing_users():
    responses = [
        {
            f"p{i}": {
                "login": f"User{i}",
                "avatarUrl": f"https://avatars.githubusercontent.com/User{i}",
            }
            for i in range(50)
        },
        {"p0": None},
    ]
    request = Mock(side_effect=responses)
    profiles = avatar_urls(
        request, tuple([f"User{i}" for i in range(50)] + ["missing"])
    )
    assert request.call_count == 2
    assert len(profiles) == 50
    assert "user0" in profiles
    assert "User0" not in request.call_args_list[0].args[0]
    assert request.call_args_list[0].args[1]["login0"] == "User0"
    assert request.call_args_list[1].args[1] == {"login0": "missing"}
    assert avatar_urls(request, ()) == {}


def test_commit_count_lookup_fetches_only_requested_numbers_in_batches():
    responses = [
        {
            "repository": {
                f"p{i}": {"commits": {"totalCount": i + 1}} for i in range(50)
            }
        },
        {"repository": {"p0": {"commits": {"totalCount": 51}}}},
    ]
    request = Mock(side_effect=responses)
    result = pull_request_commit_counts(request, "acme/api", list(range(51)))
    assert request.call_count == 2 and len(result) == 51 and result[50] == 51
    assert request.call_args_list[0].args[1]["n49"] == 49
    assert request.call_args_list[1].args[1] == {
        "owner": "acme",
        "name": "api",
        "n0": 50,
    }
    assert "reviews" not in request.call_args_list[0].args[0]
    assert pull_request_commit_counts(request, "acme/api", []) == {}
