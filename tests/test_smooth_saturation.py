import NanoTest as bot


def test_hill_saturation_keeps_distinguishing_large_values():
    values = [bot.smooth_saturation(value, 5.8, 2.0) for value in (5, 10, 15, 20)]

    assert 0.0 < values[0] < values[1] < values[2] < values[3] < 1.0
    assert round(values[0], 2) == 0.43
    assert round(values[1], 2) == 0.75
    assert round(values[2], 2) == 0.87
    assert round(values[3], 2) == 0.92


def test_save_stress_grows_smoothly_without_reaching_hard_ceiling():
    moderate = bot.calculate_save_stress(2, 1, 0, 0, 2, 2)
    strong = bot.calculate_save_stress(3, 2, 0, 0, 3, 3)
    extreme = bot.calculate_save_stress(6, 4, 0, 0, 6, 5)

    assert 0.0 < moderate < strong < extreme < 1.0


def test_conceded_goals_do_not_count_as_saves():
    without_goals = bot.calculate_save_stress(3, 2, 0, 0, 4, 3)
    with_goals = bot.calculate_save_stress(3, 2, 2, 1, 4, 3)

    assert with_goals == without_goals


def test_single_shot_on_target_does_not_create_save_stress():
    assert bot.calculate_save_stress(1, 0, 0, 0, 1, 0) == 0.0


def test_derived_metrics_uses_the_shared_smooth_save_formula():
    derived = bot.calculate_derived_metrics(
        xg_home=1.0,
        xg_away=0.5,
        shots_on_home=4,
        shots_on_away=3,
        shots_in_box_home=5,
        shots_in_box_away=3,
        saves_home=3,
        saves_away=2,
        current_home_score=1,
        current_away_score=1,
        possession_home=55,
        possession_away=45,
        pressure_index=18,
    )

    expected = bot.calculate_save_stress(3, 2, 1, 1, 4, 3)
    assert derived["save_stress"] == f"{expected:.4f}"

