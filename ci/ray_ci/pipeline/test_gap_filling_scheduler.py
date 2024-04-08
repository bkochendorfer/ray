from unittest import mock

from ci.ray_ci.pipeline.gap_filling_scheduler import GapFillingScheduler


@mock.patch("ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler._init_buildkite")
@mock.patch("ray_ci.pipeline.gap_filling_scheduler.GapFillingScheduler._get_builds")
def test_get_latest_revision_for_build_state(mock_init_buildkite, mock_get_builds):
    mock_get_builds.return_value = [
        {
            "number": 1,
            "state": "failed",
            "commit": "000",
        },
        {
            "number": 2,
            "state": "passed",
            "commit": "111",
        },
        {
            "number": 3,
            "state": "failed",
            "commit": "222",
        },
    ]
    scheduler = GapFillingScheduler("org", "pipeline")
    assert scheduler._get_latest_revision_for_build_state("failed") == "222"
    assert scheduler._get_latest_revision_for_build_state("passed") == "111"
