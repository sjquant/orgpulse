from orgpulse.libs.github import avatar_urls


def test_profile_lookup_batches_variables_and_ignores_missing_users():
    calls = []

    def request(query, variables):
        calls.append((query, variables))
        return {
            key: {
                "login": login,
                "avatarUrl": f"https://avatars.githubusercontent.com/{login}",
            }
            if login != "missing"
            else None
            for key, login in variables.items()
        }

    people = tuple([f"User{i}" for i in range(50)] + ["missing"])
    profiles = avatar_urls(request, people)
    assert len(calls) == 2
    assert len(profiles) == 50
    assert "user0" in profiles
    assert "User0" not in calls[0][0]
    assert calls[0][1]["login0"] == "User0"
    assert avatar_urls(request, ()) == {}
