import abc
import subprocess
from typing import List, Optional

from ci.ray_ci.utils import logger


class Bisector(abc.ABC):
    def __init__(
        self,
        test: str,
        passing_revision: str,
        failing_revision: str,
    ) -> None:
        self.test = test
        self.passing_revision = passing_revision
        self.failing_revision = failing_revision

    def run(self) -> Optional[str]:
        """
        Find the blame revision for the test given the range of passing and failing
        revision. If a blame cannot be found, return None
        """
        revisions = self._get_revision_lists()
        if len(revisions) < 2:
            return None
        while len(revisions) > 2:
            logger.info(
                f"Bisecting between {len(revisions)} revisions: "
                f"{revisions[0]} to {revisions[-1]}"
            )
            mid = len(revisions) // 2
            if self.check_out_and_validate(revisions[mid]):
                revisions = revisions[mid:]
            else:
                revisions = revisions[:mid]

        return revisions[-1]

    def _get_revision_lists(self) -> List[str]:
        return (
            subprocess.check_output(
                f"git rev-list --reverse "
                f"^{self.passing_revision}~ {self.failing_revision}",
                shell=True,
            )
            .decode("utf-8")
            .strip()
            .split("\n")
        )

    def check_out_and_validate(self, revision: str) -> bool:
        """
        Validate whether the test is passing or failing on the given revision
        """
        subprocess.check_call(["git", "checkout", revision])
        self.validate(revision)

    @abc.abstractmethod
    def validate(self, revision: str) -> bool:
        """
        Validate whether the test is passing or failing on the given revision
        """
        pass
