import os
import ast
from collectors.lib.collectorbase import CollectorBase

class ManualScript(CollectorBase):
    def __init__(self, config, logger, readq):
        super(ManualScript, self).__init__(config, logger, readq)
        self.command = ast.literal_eval(self.get_config("command"))

    def __call__(self):
        if len(self.command):
            for command in self.command:
                stdout = os.popen(command).read().splitlines()
                for metric in stdout:
                    self._readq.nput(metric)