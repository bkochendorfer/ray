import subprocess
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any

from pybuildkite.buildkite import Buildkite

from ci.ray_ci.utils import logger
from release.ray_release.aws import get_secret_token
from release.ray_release.configs.global_config import AWS_SECRET_BUILDKITE


MASTER_BRANCH = "master"


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

    def run(self, dry_run: bool) -> None:
        """
        Create gap filling builds for the latest failing build. If dry_run is True,
        print the commits for each build but no builds will actually be created.
        """
        revisions = self._get_gap__get_gap_revisionscommits()
        if not revisions:
            logger.info("No recent failing builds found.")
            return

        for revision in revisions:
            logger.info("Scheduling gap filling build for commit %s", revision)
            if not dry_run:
                build_number = self._create_build(revision)
                logger.info("\tGap filling build created with number %s", build_number)

    def _create_build(self, commit: str) -> str:
        build = self.ray_buildkite.builds().create_build(
            self.buildkite_organization,
            self.buildkite_pipeline,
            commit,
            MASTER_BRANCH,
        )
        return build["number"]

    def _get_gap_revisions(self) -> List[str]:
        failing_revision = self._get_latest_revision_for_build_state("failed")
        passing_revision = self._get_latest_revision_for_build_state("passed")
        return (
            subprocess.check_output(
                f"git rev-list --reverse ^{passing_revision} {failing_revision}~",
                shell=True,
            )
            .decode("utf-8")
            .strip()
            .split("\n")
        )

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
