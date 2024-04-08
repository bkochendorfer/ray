import subprocess
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

from pybuildkite.buildkite import Buildkite

from ci.ray_ci.utils import logger
from release.ray_release.aws import get_secret_token
from release.ray_release.configs.global_config import AWS_SECRET_BUILDKITE


class GapFillingScheduler:
    """
    This buildkite pipeline scheduler is responsible for scheduling gap filling builds
    when the latest build is failing.
    """

    buildkite = None

    def __init__(
        self,
        buildkite_organization: str,
        buildkite_pipeline: str,
    ):
        self.buildkite_organization = buildkite_organization
        self.buildkite_pipeline = buildkite_pipeline
        GapFillingScheduler._init_buildkite()

    @classmethod
    def _init_buildkite(cls):
        if not cls.buildkite:
            cls.buildkite = Buildkite()
            cls.buildkite.set_access_token(get_secret_token(AWS_SECRET_BUILDKITE))

    def _get_latest_revision_for_build_state(self, build_state: str) -> Optional[str]:
        builds = sorted(
            [build for build in self._get_builds() if build["state"] == build_state],
            key=lambda build: build["number"],
            reverse=True,
        )
        if not builds:
            return None

        return builds[0]["commit"]

    def _get_builds(self, days_ago: int = 1) -> List[Dict[str, Any]]:
        return self.buildkite.builds().list_all_for_pipeline(
            self.buildkite_organization,
            self.buildkite_pipeline,
            created_from=datetime.now() - timedelta(days=days_ago),
            branch=MASTER_BRANCH,
        )
