import NanoTest as bot


def _fixture(
    *,
    minute=54,
    total_shots=(14, 6),
    shots_on_target=(4, 2),
    shots_off_target=(6, 2),
    blocked_shots=(4, 2),
    corners=(5, 2),
    possession=(62, 38),
    offsides=(2, 1),
    xg=(1.4, 0.6),
):
    return {
        "elapsed": {"value": minute},
        "total_shots_home": {"value": total_shots[0]},
        "total_shots_away": {"value": total_shots[1]},
        "shots_on_target_home": {"value": shots_on_target[0]},
        "shots_on_target_away": {"value": shots_on_target[1]},
        "shots_off_target_home": {"value": shots_off_target[0]},
        "shots_off_target_away": {"value": shots_off_target[1]},
        "blocked_shots_home": {"value": blocked_shots[0]},
        "blocked_shots_away": {"value": blocked_shots[1]},
        "corner_kicks_home": {"value": corners[0]},
        "corner_kicks_away": {"value": corners[1]},
        "ball_possession_home": {"value": possession[0]},
        "ball_possession_away": {"value": possession[1]},
        "offsides_home": {"value": offsides[0]},
        "offsides_away": {"value": offsides[1]},
        "expected_goals_home": {"value": xg[0]},
        "expected_goals_away": {"value": xg[1]},
    }


def test_pressure_index_v3_stays_on_compatible_zero_to_25_scale():
    quiet = bot.calculate_pressure_index_details(
        _fixture(
            total_shots=(4, 3),
            shots_on_target=(1, 1),
            shots_off_target=(2, 1),
            blocked_shots=(1, 1),
            corners=(1, 1),
            possession=(51, 49),
            offsides=(0, 1),
        )
    )
    active = bot.calculate_pressure_index_details(_fixture())

    assert quiet["pressure_index_version"] == "v3_smooth"
    assert 0.0 <= quiet["pressure_index"] < active["pressure_index"] <= 25.0
    assert active["pressure_index"] >= 18.0


def test_pressure_index_no_longer_gets_constant_ten_points_from_possession():
    possession_only = bot.calculate_pressure_index_details(
        _fixture(
            total_shots=(0, 0),
            shots_on_target=(0, 0),
            shots_off_target=(0, 0),
            blocked_shots=(0, 0),
            corners=(0, 0),
            possession=(65, 35),
            offsides=(0, 0),
        )
    )

    assert 0.0 < possession_only["pressure_index"] < 5.0
    assert possession_only["corner_component"] == 0.0
    assert possession_only["attempt_component"] == 0.0


def test_pressure_index_is_independent_of_xg():
    low_xg = bot.calculate_pressure_index_details(_fixture(xg=(0.1, 0.1)))
    high_xg = bot.calculate_pressure_index_details(_fixture(xg=(3.0, 2.0)))

    assert high_xg["pressure_index"] == low_xg["pressure_index"]


def test_pressure_index_accounts_for_elapsed_time():
    early = bot.calculate_pressure_index_details(_fixture(minute=45))
    later = bot.calculate_pressure_index_details(_fixture(minute=70))

    assert early["pressure_index"] > later["pressure_index"]


def test_pressure_components_sum_to_reported_index_before_rounding_tolerance():
    details = bot.calculate_pressure_index_details(_fixture())
    component_sum = sum(
        details[key]
        for key in (
            "attempt_component",
            "corner_component",
            "dominance_component",
            "offside_component",
        )
    )

    assert abs(details["pressure_index"] - component_sum) < 0.01


def test_pressure_index_scalar_wrapper_matches_details():
    fixture = _fixture()

    assert bot.calculate_pressure_index(fixture) == bot.calculate_pressure_index_details(fixture)["pressure_index"]


def test_api_sports_shots_off_goal_maps_to_pressure_field():
    client = bot.APISportsMetricsClient.__new__(bot.APISportsMetricsClient)

    assert client._map_stat_name("Shots off Goal") == "shots_off_target"


def test_pressure_components_approach_but_do_not_hit_hard_ceiling():
    extreme = bot.calculate_pressure_index_details(
        _fixture(
            minute=46,
            total_shots=(40, 25),
            shots_on_target=(12, 8),
            shots_off_target=(18, 10),
            blocked_shots=(10, 7),
            corners=(15, 10),
            possession=(80, 20),
            offsides=(8, 6),
        )
    )

    assert 20.0 < extreme["pressure_index"] < 25.0
    assert extreme["attempt_pace_norm"] < 1.0
    assert extreme["corner_pace_norm"] < 1.0
    assert extreme["offside_pace_norm"] < 1.0
