import os
import subprocess

from ci.ray_ci.bisector import Bisector


class MacOSBisector(Bisector):
    def validate(self, revision: str) -> bool:
        return (
            subprocess.run(
                ["./ci/ray_ci/macos_ci_test.sh", self.test],
                cwd=os.environ["RAYCI_CHECKOUT_DIR"],
            ).returncode
            == 0
        )
